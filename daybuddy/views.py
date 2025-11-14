import calendar, datetime
from collections import defaultdict

from django.conf import settings
import json as _json
from pathlib import Path as _Path
CAL_SOURCES_PATH = _Path('/etc/houseboard_cal_sources.json')

def _read_cal_sources():
    try:
        return _json.loads(CAL_SOURCES_PATH.read_text())
    except Exception:
        return getattr(settings, 'CAL_SOURCES', [])

from django.core.cache import cache
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

import requests
from icalendar import Calendar
from dateutil.rrule import rrulestr

import os, sqlite3, mimetypes
from pathlib import Path
from collections import deque
from django.http import JsonResponse, FileResponse, Http404
from django.views.decorators.http import require_GET
from django.views.decorators.http import require_http_methods, require_POST
from django.utils.text import slugify



PHOTOS_ROOT = Path("/srv/daybuddy/photos")
READY_ROOT  = PHOTOS_ROOT / "ready"
DB_PATH     = PHOTOS_ROOT / "index.sqlite3"

# Static assets path for the always-visible upload QR code
STATIC_DIR = Path("/srv/daybuddy/static")
QR_PATH = STATIC_DIR / "daybuddy_upload_qr.png"

# Upload destination and simple PIN gate for mobile uploads
UPLOAD_DIR = PHOTOS_ROOT / "incoming" / "upload"
UPLOAD_PIN = os.environ.get("DAYBUDDY_UPLOAD_PIN", getattr(settings, "DAYBUDDY_UPLOAD_PIN", "2468"))

# Keep a small ring buffer of recently shown ids to reduce repeats
RECENT_IDS = deque(maxlen=200)


def healthz(_request):
    return HttpResponse("ok")


def _month_bounds(year, month):
    first = datetime.date(year, month, 1)
    _, last_day = calendar.monthrange(year, month)
    last = datetime.date(year, month, last_day)
    # pad to full 6x7 grid (start on Sunday)
    start = first - datetime.timedelta(days=(first.weekday() + 1) % 7)
    end = last + datetime.timedelta(days=(6 - last.weekday()) % 7)
    return start, end

def _week_bounds(anchor: datetime.date):
    """Return (start, end) for the week containing anchor; week starts on Monday."""
    start = anchor - datetime.timedelta(days=anchor.weekday())
    end = start + datetime.timedelta(days=6)
    return start, end


def _fetch_cal(url: str) -> Calendar:
    r = requests.get(url, timeout=12)
    r.raise_for_status()
    return Calendar.from_ical(r.text)


def _expand(cal: Calendar, start: datetime.date, end: datetime.date, person: dict):
    """Expand VEVENTs into per-day entries within [start, end]."""
    evts = []
    for comp in cal.walk("vevent"):
        try:
            summary = str(comp.get("summary", ""))
            dtstart = comp.decoded("dtstart")
            dtend = comp.decoded("dtend") if comp.get("dtend") else dtstart

            # normalize to date-only for boundaries
            ds = dtstart.date() if isinstance(dtstart, datetime.datetime) else dtstart
            de = dtend.date() if isinstance(dtend, datetime.datetime) else dtend

            # recurrence
            if comp.get("rrule"):
                # Build an rrulestr; comp.get('rrule') returns dict of lists (values may be bytes/dates/datetimes)
                def _fmt_rr_val(val):
                    import datetime as _dt
                    # icalendar may yield bytes, strings, dates, or datetimes (e.g., UNTIL)
                    if isinstance(val, bytes):
                        return val.decode('utf-8')
                    if isinstance(val, _dt.datetime):
                        # Convert to UTC Zulu if tz-aware; otherwise format as local naive
                        if val.tzinfo:
                            val = val.astimezone(_dt.timezone.utc).replace(tzinfo=None)
                            return val.strftime('%Y%m%dT%H%M%SZ')
                        return val.strftime('%Y%m%dT%H%M%S')
                    if isinstance(val, _dt.date):
                        return val.strftime('%Y%m%d')
                    return str(val)
    
                parts = []
                for k, vlist in comp.get("rrule").items():
                    vals = ",".join(_fmt_rr_val(v) for v in vlist if v is not None)
                    parts.append(f"{k}={vals}")
                rule_str = "\n".join(parts)
                rule = rrulestr(
                    rule_str,
                    dtstart=(
                        dtstart
                        if isinstance(dtstart, datetime.datetime)
                        else datetime.datetime.combine(ds, datetime.time.min)
                    ),
                )
                for occ in rule.between(
                    datetime.datetime.combine(start, datetime.time.min),
                    datetime.datetime.combine(end, datetime.time.max),
                    inc=True,
                ):
                    evts.append(
                        dict(
                            pid=person.get("id"),
                            who=person.get("name"),
                            color=person.get("color"),
                            date=occ.date(),
                            all_day=not isinstance(dtstart, datetime.datetime),
                            time_label=(
                                occ.strftime("%-I:%M%p").lower()
                                if isinstance(dtstart, datetime.datetime)
                                else ""
                            ),
                            title=summary,
                        )
                    )
            else:
                # slice multi-day across each day (ICS often has exclusive dtend for all-day)
                cur = max(start, ds)
                last = min(end, de if de > ds else ds)
                while cur <= last:
                    evts.append(
                        dict(
                            pid=person.get("id"),
                            who=person.get("name"),
                            color=person.get("color"),
                            date=cur,
                            all_day=not isinstance(dtstart, datetime.datetime),
                            time_label=(
                                dtstart.strftime("%-I:%M%p").lower()
                                if isinstance(dtstart, datetime.datetime) and cur == ds
                                else ""
                            ),
                            title=(summary if cur == ds else ""),
                        )
                    )
                    cur += datetime.timedelta(days=1)
        except Exception as e:
            # keep going but log parsing issue
            print(f"[DayBuddy] Parse error for {person.get('name')} event: {e}")
            continue
    return evts


def _month_matrix(year, month):
    key = f"hb:month:{year:04d}-{month:02d}"
    cached = cache.get(key)
    if cached:
        return cached

    start, end = _month_bounds(year, month)

    all_evts = []
    for person in (settings.CAL_SOURCES or []):
        try:
            cal = _fetch_cal(person["ics_url"])
            src_events = _expand(cal, start, end, person)
            print(
                f"[DayBuddy] Source '{person.get('name')}' -> {len(src_events)} events in range"
            )
            all_evts += src_events
        except Exception as e:
            print(f"[DayBuddy] Fetch error for source '{person.get('name')}': {e}")
            continue

    # bucket per day
    days = defaultdict(list)
    for e in all_evts:
        days[e["date"]].append(e)

    # sort per day: all-day first, then by time label
    for d in days:
        days[d].sort(key=lambda e: (not e["all_day"], e["time_label"] or ""))

    data = dict(
        start=start,
        end=end,
        days={d.isoformat(): days[d] for d in sorted(days)},
    )
    cache.set(key, data, 300)
    return data

def _range_matrix(start: datetime.date, end: datetime.date):
    key = f"hb:range:{start.isoformat()}:{end.isoformat()}"
    cached = cache.get(key)
    if cached:
        return cached

    all_evts = []
    for person in (settings.CAL_SOURCES or []):
        try:
            cal = _fetch_cal(person["ics_url"])
            src_events = _expand(cal, start, end, person)
            print(f"[DayBuddy] Source '{person.get('name')}' -> {len(src_events)} events in range")
            all_evts += src_events
        except Exception as e:
            print(f"[DayBuddy] Fetch error for source '{person.get('name')}': {e}")
            continue

    days = defaultdict(list)
    for e in all_evts:
        days[e["date"]].append(e)

    for d in days:
        days[d].sort(key=lambda e: (not e["all_day"], e["time_label"] or ""))

    data = dict(
        start=start,
        end=end,
        days={d.isoformat(): days[d] for d in sorted(days)},
    )
    cache.set(key, data, 300)
    return data

def month_view(request):
    today = datetime.date.today()
    y = int(request.GET.get("y", today.year))
    m = int(request.GET.get("m", today.month))
    data = _month_matrix(y, m)

    # Build 6x7 cells server-side so the template stays simple
    cells = []
    cur = data["start"]
    for _ in range(42):
        key = cur.isoformat()
        cells.append(
            {
                "date_str": key,
                "pretty": cur.strftime("%a %b %-d"),
                "events": data["days"].get(key, []),
            }
        )
        cur += datetime.timedelta(days=1)

    # Refresh event display names and per-calendar font sizes using current CAL_SOURCES
    srcs = _read_cal_sources()
    id_to_name = {p.get('id'): p.get('name') for p in srcs}
    id_to_size = {p.get('id'): int(p.get('font_size', 16) or 16) for p in srcs}
    id_to_color = {p.get('id'): p.get('color') for p in srcs}
    for c in cells:
        for e in c["events"]:
            pid = e.get("pid")
            if pid in id_to_name:
                e["who"] = id_to_name[pid]
            e["font_size"] = id_to_size.get(pid, 16)
            if id_to_color.get(pid):
                e['color'] = id_to_color[pid]

    return render(
        request,
        "daybuddy/month.html",
        {
            "year": y,
            "month": m,
            "cells": cells,
            "people": [
                {
                    "id": p.get("id"),
                    "name": p.get("name"),
                    "color": p.get("color"),
                    "font_size": int(p.get("font_size", 16) or 16),
                }
                for p in (settings.CAL_SOURCES or [])
            ],
        },
    )

def week_view(request):
    today = datetime.date.today()
    y = int(request.GET.get("y", today.year))
    m = int(request.GET.get("m", today.month))
    d = int(request.GET.get("d", today.day))

    try:
        anchor = datetime.date(y, m, d)
    except Exception:
        anchor = today

    start, end = _week_bounds(anchor)
    data = _range_matrix(start, end)

    # Build 7 cells (Monday..Sunday)
    cells = []
    cur = data["start"]
    for _ in range(7):
        key = cur.isoformat()
        cells.append({
            "date_str": key,
            "pretty": cur.strftime("%a %b %-d"),
            "events": data["days"].get(key, []),
        })
        cur += datetime.timedelta(days=1)

    # Apply latest display name / font size / color mappings
    srcs = _read_cal_sources()
    id_to_name = {p.get('id'): p.get('name') for p in srcs}
    id_to_size = {p.get('id'): int(p.get('font_size', 16) or 16) for p in srcs}
    id_to_color = {p.get('id'): p.get('color') for p in srcs}
    for c in cells:
        for e in c["events"]:
            pid = e.get("pid")
            if pid in id_to_name:
                e["who"] = id_to_name[pid]
            e["font_size"] = id_to_size.get(pid, 16)
            if id_to_color.get(pid):
                e['color'] = id_to_color[pid]

    # Week label like "Nov 3–9, 2025" (handle month/year spans nicely)
    if start.year == end.year:
        if start.month == end.month:
            week_label = f"{start:%b} {start.day}\u2013{end.day}, {start:%Y}"
        else:
            week_label = f"{start:%b} {start.day}\u2013{end:%b} {end.day}, {start:%Y}"
    else:
        week_label = f"{start:%b} {start.day}, {start:%Y}\u2013{end:%b} {end.day}, {end:%Y}"

    return render(
        request,
        "daybuddy/week.html",
        {
            "week_label": week_label,
            "start": start,
            "end": end,
            "cells": cells,
            "people": [
                {
                    "id": p.get("id"),
                    "name": p.get("name"),
                    "color": p.get("color"),
                    "font_size": int(p.get("font_size", 16) or 16),
                }
                for p in (settings.CAL_SOURCES or [])
            ],
        },
    )


def debug_view(request):
    """Return per-source counts and sample events for the current month."""
    today = datetime.date.today()
    y = int(request.GET.get("y", today.year))
    m = int(request.GET.get("m", today.month))
    start, end = _month_bounds(y, m)

    report = []
    for person in (settings.CAL_SOURCES or []):
        entry = {
            "name": person.get("name"),
            "url": person.get("ics_url"),
            "count": 0,
            "sample": [],
        }
        try:
            cal = _fetch_cal(person["ics_url"])
            evts = _expand(cal, start, end, person)
            entry["count"] = len(evts)
            entry["sample"] = [
                {
                    "date": e["date"].isoformat(),
                    "title": e["title"],
                    "time": e["time_label"],
                }
                for e in evts[:5]
            ]
        except Exception as e:
            entry["error"] = str(e)
        report.append(entry)

    return JsonResponse(
        {
            "year": y,
            "month": m,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "sources": report,
        },
        json_dumps_params={"indent": 2},
    )


@csrf_exempt
def rename_calendar(request):
    """
    POST JSON: {"id": "<calendar_id>", "name": "<new display name>", "font_size": 14|16|18|...}
    Updates /etc/houseboard_cal_sources.json and refreshes in-memory settings.
    """
    if request.method != "POST":
        return JsonResponse({"error": "POST only"}, status=405)

    try:
        import json, os

        payload = json.loads(request.body.decode('utf-8'))
        cid = (payload.get('id') or '').strip()
        new_name = (payload.get('name') or '').strip()
        fs_raw = payload.get('font_size', None)
        try:
            new_size = int(fs_raw) if fs_raw is not None else None
        except Exception:
            new_size = None
        new_color = (payload.get('color') or '').strip()
        import re as _re
        if new_color:
            m = _re.fullmatch(r'#?([0-9a-fA-F]{6}|[0-9a-fA-F]{3})', new_color)
            if m:
                h = m.group(1)
                if len(h) == 3:
                    h = ''.join(ch*2 for ch in h)
                new_color = '#' + h.lower()
            else:
                new_color = ''

        if not cid or not new_name:
            return JsonResponse({"error": "id and name required"}, status=400)

        path = "/etc/houseboard_cal_sources.json"
        if not os.path.exists(path):
            return JsonResponse({"error": "config file not found"}, status=500)

        with open(path, "r") as f:
            data = json.load(f)

        found = False
        for item in data:
            if item.get("id") == cid:
                item["name"] = new_name
                if new_size:
                    item["font_size"] = new_size
                if new_color:
                    item["color"] = new_color
                found = True
                break
        if not found:
            return JsonResponse({"error": "id not found"}, status=404)

        with open(path, "w") as f:
            json.dump(data, f, indent=2)

        # refresh in-memory and invalidate cache so UI reflects immediately
        settings.CAL_SOURCES = data
        try:
            cache.clear()
        except Exception:
            pass

        return JsonResponse({"ok": True, "id": cid, "name": new_name, "font_size": new_size, "color": new_color or None})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)




@require_GET
def photo_random(request):
    """Return a random photo from the index as JSON: {id, url, width, height, taken_at}."""
    if not DB_PATH.exists():
        return JsonResponse({"error": "index not ready"}, status=503)

    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()

        # Count how many photos exist in total
        total = cur.execute("SELECT COUNT(*) FROM photos").fetchone()[0]

        row = None
        if total > 0:
            if RECENT_IDS:
                recent = list(RECENT_IDS)
                # Only exclude RECENT_IDS if that still leaves something to choose
                if len(recent) < total:
                    placeholders = ",".join("?" for _ in recent)
                    q = f"""
                        SELECT id, relpath, width, height, taken_at
                        FROM photos
                        WHERE id NOT IN ({placeholders})
                        ORDER BY RANDOM()
                        LIMIT 1
                    """
                    row = cur.execute(q, recent).fetchone()
            # Fallback: allow repeats when everything is in the recent buffer
            if row is None:
                row = cur.execute(
                    """
                    SELECT id, relpath, width, height, taken_at
                    FROM photos
                    ORDER BY RANDOM()
                    LIMIT 1
                    """
                ).fetchone()
    finally:
        try:
            con.close()
        except Exception:
            pass

    if not row:
        return JsonResponse({"error": "no photos"}, status=404)

    pid, relpath, w, h, taken_at = row
    # Normalize/sanitize relpath coming from DB just in case
    relpath = os.path.normpath(relpath).replace("\\", "/")
    url = f"/houseboard/daybuddy/photos/file/{relpath}"

    # Build a human-friendly label and fallback if EXIF is missing
    label = None
    try:
        if taken_at:
            dt = None
            for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.datetime.strptime(taken_at, fmt)
                    break
                except Exception:
                    pass
            if dt:
                label = dt.strftime("%b %d, %Y")
    except Exception:
        pass

    if not label:
        try:
            full = (READY_ROOT / relpath).resolve()
            ts = os.path.getmtime(full)
            label = datetime.datetime.fromtimestamp(ts).strftime("%b %d, %Y")
        except Exception:
            label = None

    RECENT_IDS.append(pid)

    return JsonResponse({
        "id": pid,
        "url": url,
        "width": w,
        "height": h,
        "taken_at": taken_at,
        "taken_label": label,
    })



@require_GET
def photo_file(request, relpath):
    """Serve the actual image file from READY_ROOT safely."""
    # Prevent path traversal
    safe_rel = os.path.normpath(relpath).lstrip("/\\")
    full = (READY_ROOT / safe_rel).resolve()
    if not str(full).startswith(str(READY_ROOT.resolve())) or not full.exists():
        raise Http404("Photo not found")

    ctype, _ = mimetypes.guess_type(full.name)
    resp = FileResponse(open(full, "rb"), content_type=ctype or "image/jpeg")
    # Optional: allow browser caching; we’ll bust cache via ?v= timestamp on client
    resp["Cache-Control"] = "public, max-age=86400"
    return resp


# Serve the persistent QR code so anyone can scan to upload.
@require_GET
def daybuddy_qr(request):
    """Serve the persistent QR code so anyone can scan to upload."""
    full = QR_PATH
    if not full.exists():
        raise Http404("QR not found")
    resp = FileResponse(open(full, "rb"), content_type="image/png")
    resp["Cache-Control"] = "public, max-age=86400"
    return resp


# iPhone-friendly photo upload flow
@require_http_methods(["GET"])
def photo_upload_form(request):
    html = f"""
    <!doctype html>
    <meta name=viewport content=\"width=device-width, initial-scale=1\">
    <title>DayBuddy – Upload Photo</title>
    <style>
      body{{background:#0b0c10;color:#f2f5f9;font:400 16px/1.5 system-ui;margin:0;padding:24px}}
      .card{{background:#1b1f29;border:1px solid #333;border-radius:12px;padding:16px;max-width:520px;margin:0 auto}}
      h1{{margin:0 0 12px 0;font-size:22px}}
      label{{display:block;margin:10px 0 6px 0;opacity:.9}}
      input[type=text],input[type=password]{{width:100%;background:#141b24;border:1px solid #283246;border-radius:8px;padding:10px;color:#f2f5f9}}
      input[type=file]{{display:block;width:100%;margin:8px 0;color:#e6eaf2}}
      button{{background:#1a2333;color:#f2f5f9;border:1px solid #2a3750;border-radius:10px;padding:10px 14px;font-weight:600;}}
      .muted{{opacity:.75;font-size:14px;margin-top:8px}}
      a{{color:#9fc4ff}}
    </style>
    <div class=card>
      <h1>Upload to DayBuddy</h1>
      <form method=\"post\" action=\"/houseboard/daybuddy/photos/upload\" enctype=\"multipart/form-data\">
        <label>PIN</label>
        <input type=\"password\" name=\"pin\" inputmode=\"numeric\" autocomplete=\"one-time-code\" placeholder=\"Enter PIN\" required>
        <label>Photo</label>
        <input type=\"file\" name=\"photo\" accept=\"image/*\" required>
        <button type=\"submit\">Upload</button>
        <div class=\"muted\">Tip: on iPhone, choose Camera or Photo Library. Duplicates are auto-ignored by DayBuddy ingest.</div>
      </form>
      <p class=\"muted\">Back to <a href=\"/houseboard/daybuddy/week/\">Week View</a></p>
    </div>
    """
    return HttpResponse(html)

@csrf_exempt
@require_POST
def photo_upload(request):
    # PIN gate
    pin = (request.POST.get("pin") or "").strip()
    if not UPLOAD_PIN or pin != UPLOAD_PIN:
        return HttpResponse("<p style='color:#f66'>Invalid PIN.</p>", status=403)

    f = request.FILES.get("photo")
    if not f:
        return HttpResponse("<p style='color:#f66'>No file provided.</p>", status=400)

    ctype = f.content_type or ""
    if not ctype.startswith("image/"):
        return HttpResponse("<p style='color:#f66'>File must be an image.</p>", status=415)
    if f.size and f.size > 50*1024*1024:
        return HttpResponse("<p style='color:#f66'>File too large.</p>", status=413)

    base = Path(getattr(f, "name", "upload.jpg")).name
    stem = slugify(base.rsplit('.',1)[0]) or "photo"
    ext = ('.' + base.rsplit('.',1)[1].lower()) if ('.' in base) else '.jpg'
    ts = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    fname = f"{ts}-{stem}{ext}"

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    outpath = UPLOAD_DIR / fname
    with outpath.open('wb') as dst:
        for chunk in f.chunks():
            dst.write(chunk)

    return HttpResponse(
        "<meta name=viewport content='width=device-width, initial-scale=1'>"
        "<p style='color:#9fe'>Uploaded ✓</p>"
        "<p><a href='/houseboard/daybuddy/photos/upload/'>Upload another</a> • "
        "<a href='/houseboard/daybuddy/week/'>Back to Week View</a></p>"
    )