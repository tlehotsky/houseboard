import calendar, datetime
from django.http import HttpResponse
from django.urls import path, include
from daybuddy import views as cal_views
from temps import views as temps_views
from daybuddy import views as daybuddy_views


def hms_view(_request):
    return HttpResponse("hms ok")

def security_view(_request):
    return HttpResponse("security ok")

urlpatterns = [
    path("houseboard/healthz/", cal_views.healthz),
    path("houseboard/daybuddy/", cal_views.month_view),
    path("houseboard/daybuddy/debug/", cal_views.debug_view),
    path("houseboard/daybuddy/debug/week/", cal_views.debug_week_view),
    path("houseboard/daybuddy/rename", cal_views.rename_calendar),
    path("houseboard/daybuddy/week/", cal_views.week_view, name="daybuddy-week"),
    path("houseboard/hms/", hms_view),
    path("houseboard/security/", security_view),
    path("houseboard/daybuddy/photos/random", cal_views.photo_random, name="db_photos_random"),
    path("houseboard/daybuddy/photos/file/<path:relpath>", cal_views.photo_file, name="db_photos_file"),
    path("houseboard/daybuddy/photos/upload", cal_views.photo_upload, name="db_upload"),
    path("houseboard/daybuddy/photos/upload/", cal_views.photo_upload_form, name="db_upload_form"),
    path("houseboard/daybuddy/qr", cal_views.daybuddy_qr, name="db_qr"),
    path("houseboard/daybuddy/nhl/rangers/schedule", cal_views.rangers_schedule, name="rangers-schedule"),
    path("houseboard/daybuddy/nfl/schedule", cal_views.nfl_schedule, name="nfl-schedule"),
    path("houseboard/daybuddy/nfl/schedule/edit/", cal_views.nfl_schedule_edit, name="nfl-schedule-edit"),
    path("houseboard/daybuddy/photos/archive", cal_views.photo_archive, name="photo_archive"),
    path("api/houseboard/temps_ingest/", temps_views.temps_ingest, name="hb-temps-ingest"),
    path("api/houseboard/house_temps/", daybuddy_views.house_temps, name="house_temps"),
    path("api/houseboard/house_status/update/", daybuddy_views.update_house_status, name="house_status_update"),
    path("api/houseboard/nfl_schedule/", cal_views.nfl_schedule, name="nfl_schedule"),    


]
