
from django.db import models
import datetime


class HouseTempReading(models.Model):
    source = models.CharField(max_length=32)          # e.g. "hb-pi"
    node_id = models.CharField(max_length=64)
    sensor_serial = models.CharField(max_length=64)
    temp_c = models.FloatField()
    recorded_at = models.DateTimeField()             # from ESP
    received_at = models.DateTimeField()             # from hb-pi
    local_id = models.IntegerField()                 # id from hb-pi SQLite
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["sensor_serial", "recorded_at"]),
        ]
        unique_together = ("source", "local_id")     # prevents dup inserts

    def __str__(self):
        return f"{self.source}:{self.node_id} {self.sensor_serial} {self.temp_c}°C"