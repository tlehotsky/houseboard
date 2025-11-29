from django.shortcuts import render
import json
import datetime
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import HouseTempReading


def _parse_iso(dt_str: str):
    """
    Parse ISO8601 string to aware UTC datetime.
    If no timezone info, assume UTC.
    """
    if not dt_str:
        return None
    dt = datetime.datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


@csrf_exempt
def temps_ingest(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST only"}, status=405)

    try:
        body = request.body.decode("utf-8")
        data = json.loads(body)
    except Exception:
        return JsonResponse({"error": "invalid JSON"}, status=400)

    source = data.get("source", "hb-pi")
    readings = data.get("readings", [])

    if not isinstance(readings, list) or not readings:
        return JsonResponse({"status": "ok", "count": 0})

    objs = []
    for r in readings:
        try:
            local_id = r.get("local_id")
            node_id = r.get("node_id")
            sensor_serial = r.get("sensor_serial")
            temp_c = r.get("temp_c")
            recorded_at = _parse_iso(r.get("recorded_at"))
            received_at = _parse_iso(r.get("received_at"))

            if sensor_serial is None or temp_c is None:
                continue

            objs.append(
                HouseTempReading(
                    source=source,
                    node_id=node_id or "",
                    sensor_serial=sensor_serial,
                    temp_c=temp_c,
                    recorded_at=recorded_at
                        or datetime.datetime.now(datetime.timezone.utc),
                    received_at=received_at
                        or datetime.datetime.now(datetime.timezone.utc),
                    local_id=local_id or 0,
                )
            )
        except Exception:
            # Skip bad row, don’t kill whole batch
            continue

    created_count = 0
    if objs:
        created = HouseTempReading.objects.bulk_create(
            objs, ignore_conflicts=True
        )
        # created is a list in some Django versions, int-like in others
        created_count = len(created) if isinstance(created, list) else created

    return JsonResponse(
        {
            "status": "ok",
            "source": source,
            "received": len(readings),
            "stored": created_count,
        }
    )
