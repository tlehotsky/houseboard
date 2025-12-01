#!/usr/bin/env python3
import csv
import datetime as dt
import sys
from pathlib import Path

import requests

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
    TZ_NY = ZoneInfo("America/New_York")
except Exception:
    TZ_NY = None  # Fallback if zoneinfo not available


# --- Config -------------------------------------------------------------

TEAM_ID = 3  # New York Rangers in NHL Stats API
SEASON = "20252026"  # 2025–2026 NHL season
OUT_PATH = Path("/srv/daybuddy/data/ny_rangers_2025_2026_schedule.csv")

SCHEDULE_URL = "https://statsapi.web.nhl.com/api/v1/schedule"
# We ask the API to hydrate team & broadcast info
EXPAND = "schedule.teams,schedule.broadcasts.all"


# --- Helpers ------------------------------------------------------------

def to_local_ny(game_date_str: str) -> dt.datetime:
    """
    Convert NHL API gameDate (UTC) to approximate local time in New York.
    gameDate examples: '2025-10-10T23:00:00Z'
    """
    try:
        # Strip the trailing Z and parse as UTC
        if game_date_str.endswith("Z"):
            game_date_str = game_date_str[:-1]
        dt_utc = dt.datetime.fromisoformat(game_date_str)
        dt_utc = dt_utc.replace(tzinfo=dt.timezone.utc)
        if TZ_NY is not None:
            return dt_utc.astimezone(TZ_NY)
        # Fallback: just subtract 5 hours (rough EST approximation)
        return (dt_utc - dt.timedelta(hours=5)).replace(tzinfo=None)
    except Exception:
        # If anything goes wrong, just return naive datetime
        return dt.datetime.fromisoformat(game_date_str.replace("Z", ""))


def extract_broadcasts(game: dict) -> str:
    """
    Pull a simple comma-separated broadcast string from the hydrated schedule.
    With expand=schedule.broadcasts.all we expect a 'broadcasts' list per game.
    """
    broadcasts = game.get("broadcasts") or []
    names = []

    for b in broadcasts:
        # The NHL API typically has 'name' for the network, e.g., 'MSG', 'ESPN'
        name = (b.get("name") or "").strip()
        if name and name not in names:
            names.append(name)

    return ", ".join(names)


def extract_rangers_side(game: dict):
    """
    Given a game dict, return (home_away, opponent_name) for the Rangers,
    or (None, None) if this isn't actually a Rangers game (defensive check).
    """
    teams = game.get("teams") or {}
    home = teams.get("home", {}).get("team", {}) or {}
    away = teams.get("away", {}).get("team", {}) or {}

    home_id = home.get("id")
    away_id = away.get("id")

    if home_id == TEAM_ID:
        return "Home", away.get("name", "Unknown")
    elif away_id == TEAM_ID:
        return "Away", home.get("name", "Unknown")
    else:
        return None, None


# --- Main fetch + write -------------------------------------------------

def build_rangers_schedule():
    params = {
        "teamId": TEAM_ID,
        "season": SEASON,
        "expand": EXPAND,
        # You *can* also constrain by date if needed:
        # "startDate": "2025-09-01",
        # "endDate": "2026-06-30",
    }

    print(f"[Rangers] Fetching schedule: {SCHEDULE_URL} with {params}")
    resp = requests.get(SCHEDULE_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    dates = data.get("dates") or []
    print(f"[Rangers] Got {len(dates)} date buckets from NHL API")

    rows = []

    for day in dates:
        games = day.get("games") or []
        for game in games:
            home_away, opponent = extract_rangers_side(game)
            if not home_away:
                # Not a Rangers game; skip (defensive only)
                continue

            game_date_str = game.get("gameDate")
            if not game_date_str:
                continue

            local_dt = to_local_ny(game_date_str)
            date_str = local_dt.date().isoformat()  # YYYY-MM-DD
            time_str = local_dt.strftime("%-I:%M %p") if sys.platform != "win32" else local_dt.strftime("%I:%M %p").lstrip("0")

            status = (game.get("status") or {}).get("detailedState", "").strip()
            broadcast = extract_broadcasts(game)

            rows.append(
                {
                    "date": date_str,
                    "time": time_str,
                    "opponent": opponent,
                    "home_away": home_away,
                    "status": status,
                    "broadcast": broadcast,
                }
            )

    # Sort by date then time
    rows.sort(key=lambda r: (r["date"], r["time"]))

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"[Rangers] Writing {len(rows)} games to {OUT_PATH}")

    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["date", "time", "opponent", "home_away", "status", "broadcast"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print("[Rangers] Done.")


if __name__ == "__main__":
    build_rangers_schedule()
