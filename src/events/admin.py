from django.contrib import admin

from events.models import EventLog


@admin.register(EventLog)
class EventLogAdmin(admin.ModelAdmin):
    pass
