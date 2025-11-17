


from django.views.decorators.http import require_GET
import calendar, datetime, csv
from collections import defaultdict
from functools import lru_cache

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


# Helper: cache reverse geocode (lat, lon) -> city, state using Nominatim
@lru_cache(maxsize=512)
def _reverse_city_state(lat_rounded, lon_rounded):
    """
    Best-effort reverse geocode (lat, lon) -> 'City, State' using Nominatim.
    lat_rounded and lon_rounded should be short floats (e.g., rounded to 4 decimals)
    so the cache can be effective.
    """
    try:
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            "format": "jsonv2",
            "lat": str(lat_rounded),
            "lon": str(lon_rounded),
            "zoom": "10",
            "addressdetails": "1",
        }
        headers = {
            "User-Agent": "daybuddy/1.0 (+https://example.com)"
        }
        resp = requests.get(url, params=params, headers=headers, timeout=3)
        if not resp.ok:
            return None
        data = resp.json()
        addr = data.get("address", {}) or {}
        city = (
            addr.get("city")
            or addr.get("town")
            or addr.get("village")
            or addr.get("hamlet")
        )
        state = addr.get("state")
        if city and state:
            return f"{city}, {state}"
        if state:
            return state
        return city or None
    except Exception:
        return None




# --- Rangers schedule API for DayBuddy card ---
# --- Rangers schedule API for DayBuddy card (CSV-based) ---
@require_GET
def rangers_schedule(request):
    """
    Return New York Rangers schedule for the next 7 days based on a local CSV file.

    CSV path: /srv/daybuddy/data/ny_rangers_2025_2026_schedule.csv

    Expected CSV header (case-insensitive, flexible):
      - date        (e.g. 2025-11-14 or 11/14/2025)
      - time        (e.g. 7:00 PM)
      - opponent    (e.g. New Jersey Devils)
      - home_away   (e.g. Home or Away)
      - status      (optional, e.g. Final, Scheduled)
    """
    csv_path = Path("/srv/daybuddy/data/ny_rangers_2025_2026_schedule.csv")
    if not csv_path.exists():
        # Fail soft: empty list, but valid JSON
        return JsonResponse({"games": []})

    today = datetime.date.today()
    end = today + datetime.timedelta(days=6)

    def first(d, *names):
        """Return first non-empty field from possible column names."""
        for name in names:
            if name in d and d[name]:
                return d[name]
        return ""

    games_out = []

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_date = first(row, "date", "Date", "DATE")
            raw_time = first(row, "time", "Time", "TIME")
            opponent = first(row, "opponent", "Opponent", "OPP", "Opp")
            home_away = first(row, "home_away", "Home/Away", "HomeAway", "HA")
            status = first(row, "status", "Status")

            if not raw_date:
                continue

            # --- Parse date from CSV ---
            date_obj = None
            raw_date_stripped = raw_date.strip()
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
                try:
                    date_obj = datetime.datetime.strptime(raw_date_stripped, fmt).date()
                    break
                except ValueError:
                    continue
            if date_obj is None:
                # Skip unparseable dates
                continue

            # --- Filter to next 7 days window ---
            if not (today <= date_obj <= end):
                continue

            # --- Parse / normalize time label ---
            time_label = raw_time.strip() if raw_time else ""
            if raw_time:
                raw_t = raw_time.strip()
                parsed = False
                for fmt in ("%I:%M %p", "%I:%M%p", "%H:%M"):
                    try:
                        tdt = datetime.datetime.strptime(raw_t, fmt)
                        # Format as 7:00 PM
                        time_label = tdt.strftime("%-I:%M %p")
                        parsed = True
                        break
                    except ValueError:
                        continue
                if not parsed:
                    time_label = raw_t  # fall back to raw CSV text

            # --- Display labels ---
            date_label = date_obj.strftime("%b %-d")   # e.g. Nov 14
            day_label = date_obj.strftime("%a")        # e.g. Fri

            games_out.append({
                "date": date_obj.isoformat(),
                "date_label": date_label,
                "day_label": day_label,
                "time_label": time_label,
                "home_away": home_away,
                "opponent": opponent,
                "status": status,
            })

    games_out.sort(key=lambda g: (g["date"], g["time_label"] or ""))

    return JsonResponse({"games": games_out})


@require_GET
def nfl_schedule(request):
    """
    Return NFL schedule for the next 7 days based on the local season CSV.

    Looks for one of the following files under /srv/daybuddy/data:
      - nfl_schedule_2025.csv
      - nfl_2025_full.csv

    Expected columns (case-insensitive, flexible):
      - Home Team / Away Team
      - Date  (e.g. 05/09/2025 00:20 or 09/05/2025 00:20)

    Output format is similar to the Rangers endpoint, but "opponent" is a
    simple "Home vs Away" label, with no extra prefixes.
    """
    base_dir = Path("/srv/daybuddy/data")
    candidates = [
        base_dir / "nfl_schedule_2025.csv",
        base_dir / "nfl_2025_full.csv",
    ]
    csv_path = None
    for p in candidates:
        if p.exists():
            csv_path = p
            break
    if not csv_path:
        return JsonResponse({"games": []})

    today = datetime.date.today()
    end = today + datetime.timedelta(days=6)

    def first(d, *names):
        """Return first non-empty field from possible column names."""
        for name in names:
            if name in d and d[name]:
                return d[name]
        return ""

    def nickname(name: str) -> str:
        parts = (name or "").split()
        return parts[-1] if parts else (name or "")

    games_out = []

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_dt = first(row, "Date", "date", "DATE")
            if not raw_dt:
                continue

            # Parse date/time; CSV has strings like "05/09/2025 00:20".
            dt_obj = None
            raw_dt_stripped = raw_dt.strip()
            for fmt in (
                "%d/%m/%Y %H:%M",
                "%m/%d/%Y %H:%M",
                "%Y-%m-%d %H:%M",
                "%d/%m/%Y",
                "%m/%d/%Y",
                "%Y-%m-%d",
            ):
                try:
                    dt_obj = datetime.datetime.strptime(raw_dt_stripped, fmt)
                    break
                except ValueError:
                    continue
            if dt_obj is None:
                continue

            date_obj = dt_obj.date()

            # Only keep games in the next 7 calendar days (including today)
            if not (today <= date_obj <= end):
                continue

            home_team = first(row, "Home Team", "Home", "home", "HOME TEAM") or "Home"
            away_team = first(row, "Away Team", "Away", "away", "AWAY TEAM") or "Away"

            # Use only the nickname (last word), e.g. "Eagles" from "Philadelphia Eagles"
            matchup = f"{nickname(home_team)} vs {nickname(away_team)}".strip()

            date_label = date_obj.strftime("%b %-d")   # e.g. Nov 15
            day_label = date_obj.strftime("%a")        # e.g. Sat

            games_out.append(
                {
                    "date": date_obj.isoformat(),
                    "date_label": date_label,
                    "day_label": day_label,
                    # keep time_label empty for a very simple display
                    "time_label": "",
                    # no explicit home/away tag; the matchup label is enough
                    "home_away": "",
                    "opponent": matchup,
                    "status": first(row, "Result", "Status", "status"),
                }
            )

    games_out.sort(key=lambda g: (g["date"], g["opponent"] or ""))

    return JsonResponse({"games": games_out})


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

            # Normalize timezone-aware datetimes to naive local (drop tzinfo) so
            # we don't mix aware and naive datetimes in rrulestr/between().
            if isinstance(dtstart, datetime.datetime) and dtstart.tzinfo:
                dtstart = dtstart.replace(tzinfo=None)
            if isinstance(dtend, datetime.datetime) and dtend.tzinfo:
                dtend = dtend.replace(tzinfo=None)

            # normalize to date-only for boundaries
            ds = dtstart.date() if isinstance(dtstart, datetime.datetime) else dtstart
            de = dtend.date() if isinstance(dtend, datetime.datetime) else dtend

            # recurrence / single events
            rrule_data = comp.get("rrule")
            rdate_data = comp.get("rdate")
            exdate_data = comp.get("exdate")

            # Debug logging for Syd's calendar to help understand class recurrences
            try:
                if person.get("name") in ("Syd", "Sydney"):
                    print(
                        "[DayBuddy][Syd DEBUG]",
                        "summary=", summary,
                        "dtstart=", dtstart,
                        "rrule_keys=", list(rrule_data.keys()) if rrule_data else None,
                        "has_rdate=", bool(rdate_data),
                        "has_exdate=", bool(exdate_data),
                    )
            except Exception:
                pass

            if rrule_data or rdate_data:
                # Build full set of occurrence datetimes from RRULE and RDATE, then
                # subtract EXDATEs. This helps us match what Google shows for
                # school schedules and other complex repeats.
                import datetime as _dt

                def _fmt_rr_val(val):
                    """Normalize RRULE values into a dateutil-friendly string.

                    We deliberately strip timezone info from datetimes so that DTSTART
                    (which we pass in as naive) and UNTIL/other fields are all naive
                    as well. This avoids the "UNTIL/DTSTART must have the same
                    timezone" errors from dateutil.
                    """
                    # icalendar may yield bytes, strings, dates, or datetimes (e.g., UNTIL)
                    if isinstance(val, bytes):
                        return val.decode("utf-8")
                    if isinstance(val, _dt.datetime):
                        # Drop tzinfo so everything is naive (local) for dateutil
                        if val.tzinfo is not None:
                            val = val.replace(tzinfo=None)
                        return val.strftime("%Y%m%dT%H%M%S")
                    if isinstance(val, _dt.date):
                        return val.strftime("%Y%m%d")
                    return str(val)

                occurrences = []

                # --- RRULE expansion ---
                if rrule_data:
                    # rrule_data is a dict-like mapping (e.g. {'FREQ': ['WEEKLY'], 'BYDAY': ['MO', 'WE']}).
                    # Keys may come through as upper- or lower-case, so we normalize
                    # to upper-case and make sure a FREQ is present.
                    freq_key = None
                    for k in rrule_data.keys():
                        if str(k).upper() == "FREQ":
                            freq_key = k
                            break
                    if freq_key is None:
                        # Malformed RRULE; skip expansion for this component
                        print(f"[DayBuddy] Skipping malformed RRULE (no FREQ) for {person.get('name')} summary={summary!r}")
                    else:
                        parts = []
                        for k, vlist in rrule_data.items():
                            key = str(k).upper()
                            vals = ",".join(_fmt_rr_val(v) for v in vlist if v is not None)
                            if not vals:
                                continue
                            parts.append(f"{key}={vals}")
                        # Join with semicolons so dateutil.rrule.rrulestr parses correctly
                        rule_str = ";".join(parts)
                        try:
                            rule = rrulestr(
                                rule_str,
                                dtstart=(
                                    dtstart
                                    if isinstance(dtstart, _dt.datetime)
                                    else _dt.datetime.combine(ds, _dt.time.min)
                                ),
                            )
                            for occ in rule.between(
                                _dt.datetime.combine(start, _dt.time.min),
                                _dt.datetime.combine(end, _dt.time.max),
                                inc=True,
                            ):
                                occurrences.append(occ)
                        except Exception as _e:
                            print(f"[DayBuddy] RRULE parse error for {person.get('name')} summary={summary!r}: {rule_str!r} -> {_e}")

                # --- RDATE explicit dates ---
                def _extract_rdates(val):
                    out = []
                    if not val:
                        return out
                    # icalendar often gives RDATE as a vDDDLists with .dts
                    try:
                        dts = getattr(val, "dts", None)
                        if dts is not None:
                            for v in dts:
                                dtv = getattr(v, "dt", v)
                                if isinstance(dtv, _dt.datetime) and dtv.tzinfo:
                                    dtv = dtv.replace(tzinfo=None)
                                out.append(dtv)
                            return out
                    except Exception:
                        pass
                    # or as a list of such objects
                    if isinstance(val, (list, tuple)):
                        for item in val:
                            try:
                                dts = getattr(item, "dts", None)
                                if dts is not None:
                                    for v in dts:
                                        dtv = getattr(v, "dt", v)
                                        if isinstance(dtv, _dt.datetime) and dtv.tzinfo:
                                            dtv = dtv.replace(tzinfo=None)
                                        out.append(dtv)
                                else:
                                    dtv = getattr(item, "dt", item)
                                    if isinstance(dtv, _dt.datetime) and dtv.tzinfo:
                                        dtv = dtv.replace(tzinfo=None)
                                    out.append(dtv)
                            except Exception:
                                out.append(item)
                    else:
                        dtv = getattr(val, "dt", val)
                        if isinstance(dtv, _dt.datetime) and dtv.tzinfo:
                            dtv = dtv.replace(tzinfo=None)
                        out.append(dtv)
                    return out

                if rdate_data:
                    for rdt in _extract_rdates(rdate_data):
                        # Normalize to datetime and apply the original time-of-day when needed
                        if isinstance(rdt, _dt.date) and not isinstance(rdt, _dt.datetime):
                            if isinstance(dtstart, _dt.datetime):
                                rdt = _dt.datetime.combine(rdt, dtstart.timetz() if dtstart.tzinfo else dtstart.time())
                            else:
                                rdt = _dt.datetime.combine(rdt, _dt.time.min)
                        occurrences.append(rdt)

                # --- EXDATE exclusions ---
                def _extract_exdates(val):
                    out = []
                    if not val:
                        return out
                    try:
                        dts = getattr(val, "dts", None)
                        if dts is not None:
                            for v in dts:
                                dtv = getattr(v, "dt", v)
                                if isinstance(dtv, _dt.datetime) and dtv.tzinfo:
                                    dtv = dtv.replace(tzinfo=None)
                                out.append(dtv)
                            return out
                    except Exception:
                        pass
                    if isinstance(val, (list, tuple)):
                        for item in val:
                            try:
                                dts = getattr(item, "dts", None)
                                if dts is not None:
                                    for v in dts:
                                        dtv = getattr(v, "dt", v)
                                        if isinstance(dtv, _dt.datetime) and dtv.tzinfo:
                                            dtv = dtv.replace(tzinfo=None)
                                        out.append(dtv)
                                else:
                                    dtv = getattr(item, "dt", item)
                                    if isinstance(dtv, _dt.datetime) and dtv.tzinfo:
                                        dtv = dtv.replace(tzinfo=None)
                                    out.append(dtv)
                            except Exception:
                                out.append(item)
                    else:
                        dtv = getattr(val, "dt", val)
                        if isinstance(dtv, _dt.datetime) and dtv.tzinfo:
                            dtv = dtv.replace(tzinfo=None)
                        out.append(dtv)
                    return out

                exdates = set()
                if exdate_data:
                    for ex in _extract_exdates(exdate_data):
                        if isinstance(ex, _dt.datetime):
                            exdates.add(ex)
                        elif isinstance(ex, _dt.date):
                            exdates.add(_dt.datetime.combine(ex, _dt.time.min))

                # Deduplicate and apply exclusions
                seen = set()
                for occ in occurrences:
                    if isinstance(occ, _dt.date) and not isinstance(occ, _dt.datetime):
                        occ_dt = _dt.datetime.combine(occ, _dt.time.min)
                    else:
                        occ_dt = occ
                    if occ_dt in exdates:
                        continue
                    if occ_dt in seen:
                        continue
                    seen.add(occ_dt)

                    evts.append(
                        dict(
                            pid=person.get("id"),
                            who=person.get("name"),
                            color=person.get("color"),
                            date=occ_dt.date(),
                            all_day=not isinstance(dtstart, _dt.datetime),
                            time_label=(
                                occ_dt.strftime("%-I:%M%p").lower()
                                if isinstance(dtstart, _dt.datetime)
                                else ""
                            ),
                            title=summary,
                        )
                    )

            else:
                # Non-recurring: slice multi-day across each day (ICS often has
                # exclusive dtend for all-day).
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
    srcs = _read_cal_sources()
    for person in (srcs or []):
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
    srcs = _read_cal_sources()
    for person in (srcs or []):
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
                for p in (srcs or [])
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
                for p in (srcs or [])
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
    srcs = _read_cal_sources()
    for person in (srcs or []):
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


# --- DEBUG: Return all expanded events for a week ---
@require_GET
def debug_week_view(request):
    """
    Debug endpoint: return all expanded events for the week containing the given
    anchor date (y, m, d). This lets us compare what the backend sees with what
    the week card renders.
    """
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

    # Prepare per-source containers keyed by calendar id
    srcs = _read_cal_sources()
    id_to_entry = {}
    for person in (srcs or []):
        cid = person.get("id")
        entry = {
            "id": cid,
            "name": person.get("name"),
            "url": person.get("ics_url"),
            "events": [],
        }
        id_to_entry[cid] = entry

    # Walk all events in the week and bucket them by source id
    for day_key in sorted(data["days"]):
        for e in data["days"][day_key]:
            pid = e.get("pid")
            entry = id_to_entry.get(pid)
            if not entry:
                continue
            entry["events"].append(
                {
                    "date": e["date"].isoformat(),
                    "title": e["title"],
                    "who": e.get("who"),
                    "all_day": e.get("all_day"),
                    "time": e.get("time_label"),
                    "color": e.get("color"),
                }
            )

    # Compute per-source counts and build the output list
    sources_out = []
    for cid, entry in id_to_entry.items():
        entry["count"] = len(entry["events"])
        sources_out.append(entry)

    return JsonResponse(
        {
            "anchor": anchor.isoformat(),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "sources": sources_out,
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

        cols = [r[1] for r in cur.execute("PRAGMA table_info(photos)")]
        has_archived = "archived_at" in cols

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
                        SELECT id, relpath, width, height, taken_at, lat, lon
                        FROM photos
                        WHERE id NOT IN ({placeholders})
                    """
                    if has_archived:
                        q += " AND (archived_at IS NULL OR archived_at = '')"
                    q += """
                        ORDER BY RANDOM()
                        LIMIT 1
                    """
                    row = cur.execute(q, recent).fetchone()
            # Fallback: allow repeats when everything is in the recent buffer
            if row is None:
                if has_archived:
                    row = cur.execute(
                        """
                        SELECT id, relpath, width, height, taken_at, lat, lon
                        FROM photos
                        WHERE archived_at IS NULL OR archived_at = ''
                        ORDER BY RANDOM()
                        LIMIT 1
                        """
                    ).fetchone()
                else:
                    row = cur.execute(
                        """
                        SELECT id, relpath, width, height, taken_at, lat, lon
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

    pid, relpath, w, h, taken_at, lat, lon = row
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

    # Best-effort city/state label from lat/lon
    location_label = None
    if lat is not None and lon is not None:
        try:
            lat_r = round(float(lat), 4)
            lon_r = round(float(lon), 4)
            location_label = _reverse_city_state(lat_r, lon_r)
        except Exception:
            location_label = None

    return JsonResponse({
        "id": pid,
        "url": url,
        "width": w,
        "height": h,
        "taken_at": taken_at,
        "taken_label": label,
        "lat": lat,
        "lon": lon,
        "location_label": location_label,
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


# --- Archive photo endpoint ---
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

@csrf_exempt
@require_POST
def photo_archive(request):
    """
    Mark a photo as archived so it no longer appears in the slideshow.
    Body: {"id": <photo_id>}
    """
    if not DB_PATH.exists():
        return JsonResponse({"error": "index not ready"}, status=503)

    try:
        import json
        payload = json.loads(request.body.decode("utf-8") or "{}")
        pid = int(payload.get("id"))
    except Exception:
        return JsonResponse({"error": "invalid payload"}, status=400)

    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        # Ensure archived_at column exists (idempotent)
        try:
            cur.execute("ALTER TABLE photos ADD COLUMN archived_at TEXT")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e):
                raise
        # Archive this photo
        cur.execute(
            "UPDATE photos SET archived_at = CURRENT_TIMESTAMP WHERE id = ?",
            (pid,),
        )
        con.commit()
        return JsonResponse({"ok": True, "id": pid})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
    finally:
        try:
            con.close()
        except Exception:
            pass