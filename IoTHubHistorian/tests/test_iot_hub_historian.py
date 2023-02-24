import json
import os
from azure.iot.device.iothub.sync_clients import IoTHubDeviceClient
import pytest
import logging
import threading
import queue
from datetime import datetime
import time

# from volttron.platform.vip.agent import Agent
from volttron.platform.agent.base_historian import BaseHistorianAgent
from volttrontesting.utils.utils import AgentMock

from iot_hub_historian.agent import IoTHubHistorian, IoTHubMessage

IoTHubHistorian.__bases__ = (AgentMock.imitate(BaseHistorianAgent, BaseHistorianAgent()),)
_log = logging.getLogger(__name__)

event_queue = queue.Queue()


def on_event(context, event):
    event_queue.put(event)


IOT_HUB_DEVICE_CONNECTION_STRING = os.environ.get("IOT_HUB_DEVICE_CONNECTION_STRING")
INSTANCE_NAME = os.environ.get("INSTANCE_NAME")

test_config = {
    "iot_hub_device_connection_string": IOT_HUB_DEVICE_CONNECTION_STRING,
    "instance_name": INSTANCE_NAME,
}
test_event_values = {
    "physical_address": "1194/analogOutput/567",
    "instance": "the_test_site",
    "ts": datetime(year=2021, month=6, day=20, hour=3, minute=13).timestamp()
    * 1000
    * 1000
    * 1000,
    "value": "test_value",
}
test_record = {
    "topic": "1194/analogOutput/567",
    "timestamp": datetime(year=2021, month=6, day=20, hour=3, minute=13),
    "value": "test_value",
}
agent = IoTHubHistorian(config=test_config)


def test_event_format():
    test_time = datetime(year=2021, month=6, day=20, hour=3, minute=13)
    test_event = IoTHubMessage(**test_event_values)
    assert test_event.datetime == test_time
    assert test_event.value == "test_value"
    assert test_event.instance == "the_test_site"
    assert test_event.physical_address == "1194/analogOutput/567"
    assert test_event.message == json.dumps(test_event_values)

def test_event_from_record():
    native_event = IoTHubMessage(**test_event_values)
    from_record = IoTHubMessage.from_vtron_record(instance=test_event_values["instance"], record=test_record)
    assert native_event == from_record

def test_data_round_trip():
    consumer = IoTHubDeviceClient.from_connection_string(
        agent.iot_hub_device_connection_string,
    )
    worker = threading.Thread(
        target=consumer.receive,
        kwargs={
            "on_event": on_event,
            # "starting_position": "-1",  # "-1" is from the beginning of the partition.
        },
    )
    worker.start()
    time.sleep(2)
    agent.historian_setup()
    agent.publish_to_historian([test_record])
    agent.shutdown_eventhubs()

    result = event_queue.get()
    consumer.close()
    assert result.body_as_json() == test_event_values
    assert type(IoTHubMessage.from_event(result)) == IoTHubMessage
