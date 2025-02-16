from django.db import models

from core.models import TimeStampedModel


class EventLog(TimeStampedModel, models.Model):
    type = models.CharField(max_length=255)
    environment = models.CharField(max_length=50)
    context = models.JSONField()

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
        ]
