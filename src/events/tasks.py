import structlog
from celery import shared_task
from django.conf import settings
from django.db import transaction

from core.decorators import sentry_transaction
from events.client import EventLogClickhouseClient, EventLogClient
from events.models import EventLog
from events.services import EventLogService

logger = structlog.get_logger(__name__)


@sentry_transaction("process_outbox_events")
@shared_task
def process_outbox_events(event_client: EventLogClient = EventLogClickhouseClient) -> None:
    with transaction.atomic():
        events = list(
            EventLog.objects
            .select_for_update(skip_locked=True)
            .order_by('created_at')[:settings.EVENT_BATCH_SIZE],
        )

        if not events:
            return

        try:
            EventLogService.send_event(events, event_client)

            event_ids = [event.id for event in events]
            EventLog.objects.filter(id__in=event_ids).delete()

            logger.info(
                "events_processed",
                count=len(events),
            )

        except Exception as e:
            logger.error(
                "events_processing_failed",
                error=str(e),
                count=len(events),
            )
            raise e
