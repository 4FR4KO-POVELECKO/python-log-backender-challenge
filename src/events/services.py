import re
from datetime import datetime
from typing import Any

import structlog
from django.conf import settings
from django.utils import timezone

from core.base_model import Model
from events.client import EventLogClickhouseClient, EventLogClient
from events.models import EventLog

logger = structlog.get_logger(__name__)

def process_event_to_clickhouse(event: EventLog) -> tuple[Any]:
    return (
        event.type,
        event.created_at,
        event.environment,
        event.context,
    )


class EventLogService:
    PROCESS_METHOD = {
        EventLogClickhouseClient: process_event_to_clickhouse,
    }

    @classmethod
    def publish_event(
        cls,
        context: Model,
        type: str | None = None,
        environment: str = settings.ENVIRONMENT,
        created_at: datetime | None = None,
    ) -> None:
        """
        Publish an event to a table.
        """
        type = type or cls._to_snake_case(context.__class__.__name__)
        created_at = created_at or timezone.now()

        try:
            EventLog.objects.create(
                type=type,
                context=context.model_dump_json(),
                environment=environment,
                created_at=created_at,
            )
            logger.info(
                "event_published",
                event_type=type,
            )
        except Exception as e:
            logger.error(
                "event_publish_failed",
                event_type=type,
                error=str(e),
            )
            raise e

    @classmethod
    def send_event(cls, data: EventLog | list[EventLog], event_client: EventLogClient) -> None:
        if isinstance(data, EventLog):
            data = [data]

        proccessed_data = [cls.PROCESS_METHOD[event_client](event) for event in data]

        with event_client.init() as client:
            client.insert(data=proccessed_data)

    @staticmethod
    def _to_snake_case(event_name: str) -> str:
        result = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', event_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', result).lower()
