import json
from typing import Never

import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from django.conf import settings
from django.test.utils import override_settings
from django.utils import timezone

from core.base_model import BaseModel
from events.client import EventLogClickhouseClient
from events.confest import f_ch_client  # noqa
from events.models import EventLog
from events.services import EventLogService
from events.tasks import process_outbox_events

pytestmark = [pytest.mark.django_db]

class TestModel(BaseModel):
    name: str
    age: int


def fail_function(*args, **kwargs) -> Never:  # noqa
    raise DatabaseError()


def test_publish_event() -> None:
    # only model in args
    data = {'name': 'Test', 'age': 25}
    model = TestModel(**data)
    EventLogService.publish_event(model)
    event = EventLog.objects.last()

    assert event is not None
    assert event.type == EventLogService._to_snake_case(model.__class__.__name__)
    assert event.environment == settings.ENVIRONMENT
    assert json.loads(event.context) == json.loads(json.dumps(data))

    # model error
    with pytest.raises(AttributeError):
        EventLogService.publish_event('not_model')

    # custom event type
    EventLogService.publish_event(model, type='custom_type')
    event = EventLog.objects.last()
    assert event.type == 'custom_type'

    # custom event environment
    EventLogService.publish_event(model, environment='Custom')
    event = EventLog.objects.last()
    assert event.environment == 'Custom'

    # custom event datetime
    created_at = timezone.make_aware(timezone.datetime(2000, 1, 1))
    EventLogService.publish_event(model, created_at=created_at)
    event = EventLog.objects.last()
    assert event.created_at == created_at


def test_ch_end_to_end_event_processing(f_ch_client: EventLogClickhouseClient) -> None:  # noqa
    data = {'name': 'Test', 'age': 25}
    model = TestModel(**data)
    event_type = 'test'

    EventLogService.publish_event(model, event_type)

    assert EventLog.objects.count() == 1
    process_outbox_events()
    assert EventLog.objects.count() == 0

    result = f_ch_client.query("SELECT event_type, event_context FROM default.event_log WHERE event_type = 'test'")
    assert len(result) == 1
    assert result[0][0] == event_type
    assert json.loads(result[0][1]) == json.loads(json.dumps(data))

def test_process_empty_outbox() -> None:
    assert EventLog.objects.count() == 0
    process_outbox_events()
    assert EventLog.objects.count() == 0


def test_process_single_event() -> None:
    EventLog.objects.create(
        type="test",
        environment="test",
        context={"test": "data"},
    )
    process_outbox_events()
    assert EventLog.objects.count() == 0


@override_settings(EVENT_BATCH_SIZE=2)
def test_process_batch_of_events() -> None:
    for i in range(3):
        EventLog.objects.create(
            type=f"test_{i}",
            environment=f"test_{i}",
            context={"test": i},
        )

    assert EventLog.objects.count() == 3
    process_outbox_events()
    assert EventLog.objects.count() == 1
    process_outbox_events()
    assert EventLog.objects.count() == 0


def test_handle_clickhouse_error(monkeypatch: pytest.MonkeyPatch) -> None:
    EventLog.objects.create(
        type="test",
        environment="test",
        context={"test": "data"},
    )

    monkeypatch.setattr("events.services.EventLogService.send_event", fail_function)
    with pytest.raises(DatabaseError):
        process_outbox_events()

    assert EventLog.objects.count() == 1
