import calendar, datetime
from collections import defaultdict

from django.conf import settings
from django.core.cache import cache
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

import requests
from icalendar import Calendar
from dateutil.rrule import rrulestr


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
    id_to_name = {p.get("id"): p.get("name") for p in (settings.CAL_SOURCES or [])}
    id_to_size = {p.get("id"): int(p.get("font_size", 16) or 16) for p in (settings.CAL_SOURCES or [])}
    for c in cells:
        for e in c["events"]:
            pid = e.get("pid")
            if pid in id_to_name:
                e["who"] = id_to_name[pid]
            e["font_size"] = id_to_size.get(pid, 16)

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

        payload = json.loads(request.body.decode("utf-8"))
        cid = (payload.get("id") or "").strip()
        new_name = (payload.get("name") or "").strip()
        fs_raw = payload.get("font_size", None)
        try:
            new_size = int(fs_raw) if fs_raw is not None else None
        except Exception:
            new_size = None

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

        return JsonResponse({"ok": True, "id": cid, "name": new_name, "font_size": new_size})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
