from collections.abc import Generator

import pytest
from django.conf import settings

from events.client import EventLogClickhouseClient


@pytest.fixture(scope='module')
def f_ch_client() -> Generator['EventLogClickhouseClient']:
    with EventLogClickhouseClient.init() as client:
        yield client

@pytest.fixture(autouse=True)
def f_clean_up_event_log(f_ch_client: EventLogClickhouseClient) -> Generator:
    f_ch_client.query(f'TRUNCATE TABLE {settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}')
    yield
