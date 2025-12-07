


from django.views.decorators.http import require_GET
import calendar, datetime, csv
from collections import defaultdict
from functools import lru_cache
import math
import re
from django.conf import settings
from django.utils import timezone
import json as _json
from pathlib import Path as _Path
APP_ROOT = _Path(__file__).resolve().parent
CAL_SOURCES_PATH = APP_ROOT / 'cal_sources.json'

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

import json

from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST



PHOTOS_ROOT = Path("/srv/daybuddy/photos")
READY_ROOT  = PHOTOS_ROOT / "ready"
DB_PATH     = PHOTOS_ROOT / "index.sqlite3"

# Status/health JSON files (updated out-of-band)
STATUS_ROOT = Path("/srv/daybuddy/status")
NETWORK_STATUS_PATH = STATUS_ROOT / "network.json"
HOUSE_STATUS_PATH = STATUS_ROOT / "house.json"

# Static assets path for the always-visible upload QR code
STATIC_DIR = Path("/srv/daybuddy/static")
QR_PATH = STATIC_DIR / "daybuddy_upload_qr.png"

# Upload destination and simple PIN gate for mobile uploads
UPLOAD_DIR = PHOTOS_ROOT / "incoming" / "upload"
UPLOAD_PIN = os.environ.get("DAYBUDDY_UPLOAD_PIN", getattr(settings, "DAYBUDDY_UPLOAD_PIN", "2468"))

# Keep a small ring buffer of recently shown ids to reduce repeats
RECENT_IDS = deque(maxlen=200)


# --- Status/health helpers ---
def _load_status_json(path: Path):
    """
    Best-effort load of a small JSON status file. Returns {} on any error.
    This is intended to be updated out-of-band by a script on the Pi or
    elsewhere (e.g., a cron job that writes Wi-Fi and internet health).
    """
    try:
        if path.exists():
            return _json.loads(path.read_text())
    except Exception:
        pass
    return {}


def get_network_health():
    """
    Return a simple dict describing network health so the template can render
    the Network Health card.

    Expected JSON structure at NETWORK_STATUS_PATH (all keys optional):

      {
        "wifi_status": "ok|degraded|down",
        "wifi_detail": "Some human-friendly description",
        "internet_status": "ok|degraded|down",
        "internet_detail": "Description of internet connectivity",
        "latency_ms": 42,
        "updated_at": "2025-11-23T15:04:05"
      }
    """
    data = _load_status_json(NETWORK_STATUS_PATH)
    return {
        "wifi_status": data.get("wifi_status", "unknown"),
        "wifi_detail": data.get("wifi_detail", ""),
        "internet_status": data.get("internet_status", "unknown"),
        "internet_detail": data.get("internet_detail", ""),
        "latency_ms": data.get("latency_ms"),
        "updated_at": data.get("updated_at"),
    }


def get_house_health():
    """
    Return a simple dict describing house health (HVAC, temps, etc.) so the
    template can render the House Health card.

    Expected JSON at HOUSE_STATUS_PATH (all keys optional), for example:

      {
        "overall_status": "ok|alert",
        "overall_detail": "Summary line",
        "zones": [
          {"name": "Living Room", "temp_f": 70.5, "status": "ok"},
          {"name": "Upstairs", "temp_f": 68.2, "status": "cooling"}
        ],
        "updated_at": "2025-11-23T15:04:05"
      }
    """
    data = _load_status_json(HOUSE_STATUS_PATH)
    return {
        "overall_status": data.get("overall_status", "unknown"),
        "overall_detail": data.get("overall_detail", ""),
        "zones": data.get("zones", []),
        "updated_at": data.get("updated_at"),
    }

@csrf_exempt
@require_POST
def update_house_status(request):
    """
    Accept a JSON payload from hb-pi describing overall house status + zones,
    and write it to HOUSE_STATUS_PATH so get_house_health() / house_temps()
    will pick it up for the dashboard.
    Expected payload shape (flexible, but recommended):
    {
      "overall_status": "ok",
      "overall_detail": "",
      "updated_at": "2025-11-30T17:05:00Z",
      "zones": [
        {"name": "Kitchen", "temp_f": 71.2},
        {"name": "Garage", "temp_f": 62.3}
      ]
    }
    """
    try:
      raw = request.body.decode("utf-8") or "{}"
      data = json.loads(raw)
    except json.JSONDecodeError:
      return JsonResponse({"error": "Invalid JSON"}, status=400)

    # Basic defensive defaults
    overall_status = data.get("overall_status", "ok")
    overall_detail = data.get("overall_detail", "")
    zones = data.get("zones", [])

    # If caller didn’t specify updated_at, fill it in
    updated_at = data.get("updated_at")
    if not updated_at:
      updated_at = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")

    safe = {
      "overall_status": overall_status,
      "overall_detail": overall_detail,
      "updated_at": updated_at,
      "zones": zones,
    }

    # Ensure directory exists, then write JSON atomically-ish
    directory = os.path.dirname(HOUSE_STATUS_PATH)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)

    # Create a sibling temp file path like /srv/daybuddy/status/house.json.tmp
    tmp_path = HOUSE_STATUS_PATH.parent / (HOUSE_STATUS_PATH.name + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(safe, f, indent=2)
    os.replace(tmp_path, HOUSE_STATUS_PATH)
    return JsonResponse({"status": "ok"})

from django.http import JsonResponse
from django.views.decorators.http import require_GET
import json
import os

# ... your existing imports and HOUSE_STATUS_PATH definition ...


@require_GET
def house_status(request):
    """
    Read the current house status JSON file and return it as JSON.
    This is what hb-pi will poll.
    """
    try:
        with open(HOUSE_STATUS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {
            "overall_status": "unknown",
            "overall_detail": "No house status file found yet.",
            "zones": [],
        }
    except json.JSONDecodeError:
        data = {
            "overall_status": "error",
            "overall_detail": "house.json is not valid JSON",
            "zones": [],
        }

    return JsonResponse(data)


@require_GET
def house_temps(request):
    """
    Small JSON API so the House Health card in week.html can load room temps
    from the droplet.

    It adapts the status JSON from get_house_health() into a simple:
      {
        "rooms": [
          {"room": "Kitchen", "temp_f": 71.2},
          ...
        ],
        "updated_at": "2025-11-23T15:04:05"
      }
    """
    try:
        hh = get_house_health()
        zones = hh.get("zones") or []

        rooms = []
        for z in zones:
            name = z.get("name") or z.get("room") or "Room"

            raw_temp_f = z.get("temp_f")
            raw_temp_c = z.get("temp_c")

            temp_f = None

            # Prefer an explicit temp_f if present and finite
            if isinstance(raw_temp_f, (int, float)):
                try:
                    if not (math.isnan(raw_temp_f) or math.isinf(raw_temp_f)):
                        temp_f = round(float(raw_temp_f), 1)
                except TypeError:
                    temp_f = None
            # Otherwise derive °F from °C if available and finite
            elif isinstance(raw_temp_c, (int, float)):
                try:
                    if not (math.isnan(raw_temp_c) or math.isinf(raw_temp_c)):
                        temp_f = round((float(raw_temp_c) * 9.0 / 5.0) + 32.0, 1)
                except TypeError:
                    temp_f = None

            rooms.append({
                "room": name,
                "temp_f": temp_f,
            })

        return JsonResponse(
            {
                "rooms": rooms,
                "updated_at": hh.get("updated_at"),
            },
            json_dumps_params={"allow_nan": False},
        )

    except Exception as e:
        # Never emit HTML here; always JSON so jq and the frontend can handle it.
        return JsonResponse(
            {
                "rooms": [],
                "updated_at": None,
                "error": str(e),
            },
            status=500,
            json_dumps_params={"allow_nan": False},
        )

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
      - broadcast   (optional, e.g. MSG, ESPN, TNT)
    """
    csv_path = Path("/srv/daybuddy/data/ny_rangers_2025_2026_schedule.csv")
    if not csv_path.exists():
        # Fail soft: empty list, but valid JSON
        return JsonResponse({"games": []})

    today = datetime.date.today()
    end = today + datetime.timedelta(days=6)

    def classify_rangers_broadcast(label: str) -> str:
        """
        Coarse classification for TV/streaming:
          - 'msg'   -> any label containing 'MSG'
          - 'other' -> anything else (including blank)
        """
        t = (label or "").strip().upper()
        if "MSG" in t:
            return "msg"
        if not t:
            return ""
        return "other"

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
            broadcast_raw = first(row, "broadcast", "Broadcast", "BROADCAST", "Network", "NETWORK", "TV", "Tv", "tv")
            broadcast_raw = (broadcast_raw or "").strip()            

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
                "broadcast": broadcast_raw,
                "broadcast_class": classify_rangers_broadcast(broadcast_raw),
            })
            
    games_out.sort(key=lambda g: (g["date"], g["time_label"] or ""))

    return JsonResponse({"games": games_out})


# --- NFL schedule edit placeholder view ---
@require_http_methods(["GET"])
def nfl_schedule_edit(request):
    """
    Temporary placeholder page for editing the NFL schedule CSV.

    This exists so that the URL pattern
    `path("houseboard/daybuddy/nfl/schedule/edit/", cal_views.nfl_schedule_edit, ...)`
    can safely resolve without breaking the rest of DayBuddy.
    """
    return HttpResponse(
        "<!doctype html>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<title>DayBuddy – NFL Schedule Editor</title>"
        "<body style='background:#050711;color:#f5f7ff;font-family:system-ui;"
        "margin:0;padding:24px'>"
        "<div style='max-width:720px;margin:0 auto;'>"
        "<h1 style='margin:0 0 12px 0;font-size:24px;'>NFL Schedule Editor</h1>"
        "<p style='opacity:.85;font-size:15px;line-height:1.5;'>"
        "This is a placeholder page for the future NFL schedule CSV editor. "
        "The main DayBuddy dashboard should continue to work normally."
        "</p>"
        "<p style='margin-top:18px;font-size:14px;'>"
        "<a href='/houseboard/daybuddy/week/' "
        "style='color:#8fb5ff;text-decoration:none;'>"
        "← Back to Week View</a>"
        "</p>"
        "</div>"
        "</body>"
    )


def _find_nfl_csv():
    """Return the Path to the NFL CSV file if it exists, else None."""
    base_dir = Path("/srv/daybuddy/data")
    candidates = [
        base_dir / "nfl_schedule_2025.csv",
        base_dir / "nfl_2025_full.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None

@require_GET
def nfl_schedule(request):
    """
    Return JSON describing NFL schedule for the next ~7 days.

    This reads the first NFL CSV it can find via _find_nfl_csv() and
    normalises it into the simple shape that the DayBuddy week
    dashboard expects:

        {
            "games": [
                {
                    "date": "2025-12-04",
                    "day_label": "Thu",
                    "date_label": "12/4",
                    "opponent": "SEA at DAL",
                    "time_label": "8:15 PM",
                    "status": "24 - 20",
                    "broadcast_class": "prime|youtube|local|other",
                    "slot": "morning|afternoon|evening"
                },
                ...
            ]
        }

    It is tailored for fixturedownload.com CSVs like
    nfl-2025-UTC.csv which use a `Date` column of the form
    `dd/mm/YYYY HH:MM` in UTC plus `Home Team`, `Away Team`,
    `Location`, `Result`, and `Broadcast` columns.
    """
    csv_path = _find_nfl_csv()
    if not csv_path or not csv_path.exists():
        # No file available – just return an empty list so the card
        # shows "No NFL games…" instead of a 500.
        return JsonResponse({"games": []})

    today = timezone.localdate()
    window_end = today + datetime.timedelta(days=7)

    games_out = []

    # Helper to parse the fixturedownload-style Date field
    def _parse_kickoff(raw: str):
        if not raw:
            return None
        raw_str = raw.strip()
        # Try a few common patterns that include time. The primary
        # one for nfl-2025-UTC.csv is "%d/%m/%Y %H:%M".
        fmts = (
            "%d/%m/%Y %H:%M",   # 05/09/2025 00:20
            "%d/%m/%y %H:%M",
            "%Y-%m-%d %H:%M",   # 2025-09-05 00:20
            "%Y-%m-%dT%H:%M",
            "%m/%d/%Y %H:%M",   # 09/05/2025 20:20 (US style)
            "%m/%d/%y %H:%M",
            "%Y-%m-%d",         # date-only fallbacks
            "%d/%m/%Y",
            "%m/%d/%Y",
        )
        for fmt in fmts:
            try:
                return datetime.datetime.strptime(raw_str, fmt)
            except ValueError:
                continue
        return None

    # Classify TV / streaming into the small set of tags the frontend
    # expects: local, youtube, prime, other.
    def _classify_broadcast(text: str) -> str:
        t = (text or "").strip().lower()
        if not t:
            return "other"
        if "prime" in t or "amazon" in t or "tnf" in t:
            return "prime"
        if "youtube" in t:
            return "youtube"
        for key in ("fox", "cbs", "nbc", "abc", "espn"):
            if key in t:
                return "local"
        return "other"

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_dt = (row.get("Date") or row.get("date") or "").strip()
            if not raw_dt:
                continue

            dt_naive = _parse_kickoff(raw_dt)
            if not dt_naive:
                # Skip rows we can't parse instead of crashing
                continue

            # The fixturedownload CSV we pulled is in UTC (e.g. "nfl-2025-UTC"),
            # so interpret the parsed datetime as UTC and convert it to the
            # local Django timezone (America/New_York in your settings).
            try:
                import datetime as _dtmod
                dt_utc = dt_naive.replace(tzinfo=_dtmod.timezone.utc)
                local_tz = timezone.get_default_timezone()
                dt_local = dt_utc.astimezone(local_tz)
            except Exception:
                # If anything goes wrong with tz handling, fall back to naive
                dt_local = dt_naive

            game_date = dt_local.date()

            # Limit to the next ~7 days window
            if not (today <= game_date <= window_end):
                continue

            # --- Build friendly labels ---
            day_label = game_date.strftime("%a")  # Sun, Mon, ...
            date_label = f"{game_date.month}/{game_date.day}"

            home = (row.get("Home Team") or row.get("home_team") or row.get("Home") or "").strip()
            away = (row.get("Away Team") or row.get("away_team") or row.get("Away") or "").strip()

            # Prefer explicit Home/Away names from the CSV
            if home and away:
                opponent = f"{away} at {home}"
            else:
                # Fallbacks if ever needed
                opponent = (
                    (row.get("opponent") or "").strip()
                    or (row.get("Matchup") or "").strip()
                    or home
                    or away
                    or "Game"
                )

            # Time-of-day label from the CSV datetime (e.g. "1:00 PM")
            try:
                time_label = dt_local.strftime("%-I:%M %p")
            except Exception:
                # Windows / non-GNU strftime fallback (just in case)
                time_label = dt_local.strftime("%I:%M %p").lstrip("0")

            # Coarse slot classification used for card colours
            hour = dt_local.hour
            if hour < 13:
                slot = "morning"
            elif hour < 18:
                slot = "afternoon"
            else:
                slot = "evening"

            broadcast_raw = (
                row.get("Broadcast")
                or row.get("broadcast")
                or row.get("TV")
                or row.get("Network")
                or ""
            )
            broadcast_class = _classify_broadcast(broadcast_raw)

            # Use Result as a status string for now (e.g. "24 - 20")
            status = (row.get("Result") or "").strip()

            games_out.append(
                {
                    "date": game_date.isoformat(),
                    "day_label": day_label,
                    "date_label": date_label,
                    "opponent": opponent,
                    "time_label": time_label,
                    "status": status,
                    "broadcast_class": broadcast_class,
                    "slot": slot,
                }
            )

    # Sort games by date and time so the JS can just group by date
    games_out.sort(key=lambda g: (g["date"], g.get("time_label") or ""))

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
    """Return (start, end) for the week containing anchor; week starts on Saturday."""
    # Week starts on Saturday
    weekday = anchor.weekday()          # Mon=0 ... Sun=6
    offset = (weekday - 5) % 7          # Saturday = weekday 5
    start = anchor - datetime.timedelta(days=offset)
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
            "network_health": get_network_health(),
            "house_health": get_house_health(),
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

        if not CAL_SOURCES_PATH.exists():
            return JsonResponse({"error": "config file not found"}, status=500)

        with CAL_SOURCES_PATH.open("r") as f:
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

        with CAL_SOURCES_PATH.open("w") as f:
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