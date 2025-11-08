import calendar, datetime
from django.http import HttpResponse
from django.urls import path
from daybuddy import views as cal_views

def hms_view(_request):
    return HttpResponse("hms ok")

def security_view(_request):
    return HttpResponse("security ok")

urlpatterns = [
    path("houseboard/healthz/", cal_views.healthz),
    path("houseboard/daybuddy/", cal_views.month_view),
    path("houseboard/daybuddy/debug/", cal_views.debug_view),
    path("houseboard/daybuddy/rename", cal_views.rename_calendar),
    path("houseboard/daybuddy/week/", cal_views.week_view, name="daybuddy-week"),
    path("houseboard/hms/", hms_view),
    path("houseboard/security/", security_view),
]