"""
Historian agent to persist data to Azure IoT Hub.
Copyright 2021 ACE IoT Solutions LLC
(andrew@aceiotsolutions.com)
"""
from __future__ import annotations

__docformat__ = "reStructuredText"

import logging
import sys
from typing import Iterable
from dataclasses import dataclass, asdict
from datetime import datetime
from volttron.platform import jsonapi
from volttron.platform.agent.base_historian import BaseHistorianAgent
from volttron.platform.agent import utils
from volttron.platform.vip.agent import Agent, Core, RPC

from azure.iot.device import IoTHubDeviceClient, Message, IoTHubModuleClient

_log = logging.getLogger(__name__)
utils.setup_logging()
__version__ = "0.2"


@dataclass
class IoTHubMessage:
    physical_address: str
    instance: str
    ts: int
    value: str

    @classmethod
    def from_vtron_record(cls, instance: str, record: dict) -> IoTHubMessage:
        return cls(
                    physical_address=record["topic"],
                    instance=instance,
                    ts=int(record["timestamp"].timestamp() * 1000 * 1000 * 1000),
                    value=str(record["value"]),
                )
    
    @classmethod
    def from_message(cls, message: Message) -> IoTHubMessage:
        message_data = message.body_as_json()
        return cls(
            ts=message_data["ts"],
            physical_address=message_data["physical_address"],
            instance=message_data["instance"],
            value=message_data["value"]
        )


    def dump_to_json(self):
        return jsonapi.dumps(asdict(self))

    def dump_to_message(self):
        return Message(jsonapi.dumps(asdict(self)))

    @property
    def message(self):
        return self.dump_to_json()

    @property
    def iot_device_message(self) -> Message:
        return self.dump_to_message()
    
    @property
    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.ts / 1000 / 1000 / 1000)


def iot_hub_historian(config_path, **kwargs):
    """Parses the Agent configuration and returns an instance of
    the agent created using that configuration.

    :param config_path: Path to a configuration file.

    :type config_path: str
    :returns: IoTHubHistorian
    :rtype: IoTHubHistorian
    """
    try:
        config = utils.load_config(config_path)
    except BaseException:
        config = {}

    if not config:
        _log.info("Using Agent defaults for starting configuration.")

    return IoTHubHistorian(config, **kwargs)


class IoTHubHistorian(BaseHistorianAgent):
    """
    Document agent constructor here.
    """

    def __init__(self, config, **kwargs):
        super(IoTHubHistorian, self).__init__(**kwargs)
        _log.debug("vip_identity: " + str(self.core.identity))
        _log.debug("config: " + str(config))

        self.default_config = {}
        self.default_config.update(config)
        self._initialize_config(config)

        # Set a default configuration to ensure that self.configure is called immediately to setup
        # the agent.
        self.vip.config.set_default("config", self.default_config)
        # Hook self.configure up to changes to the configuration file "config".
        self.vip.config.subscribe(
            self.configure, actions=["NEW", "UPDATE"], pattern="config"
        )

    def _initialize_config(self, config):
        try:

            iot_hub_device_connection_string = str(config["iot_hub_device_connection_string"])
            instance_name = str(config["instance_name"])
        except ValueError as e:
            _log.error("ERROR PROCESSING CONFIGURATION: {}".format(e))
            return

        self.iot_hub_device_connection_string = iot_hub_device_connection_string
        self.instance_name = instance_name

    def configure(self, config_name, action, contents):
        """
        Called after the Agent has connected to the message bus. If a configuration exists at startup
        this will be called before onstart.

        Is called every time the configuration in the store changes.
        """
        config = self.default_config.copy()
        config.update(contents)

        _log.debug("Configuring Agent")
        _log.debug(contents)
        self._initialize_config(config)

    def publish_to_historian(self, to_publish_list):
        """Takes records from BaseHistorian and attempts to forward them to IoTHub"""
        completed = []
        try:
            for record in to_publish_list:
                message = IoTHubMessage.from_vtron_record(
                    instance=self.instance_name,
                    record=record
                )
                try:
                    self.device_client.send_message(message.iot_device_message)
                    completed.append(record)
                    self.report_handled(completed)
                    completed = []
                except Exception as e:
                    _log.error("Failed to send max_size batch")
                    _log.error(e)
                    raise e
        except Exception as e:
            _log.error("Batching Loop failed")
            _log.error(e)
            raise e

    def _event_hubs_connection_string(self) -> str:
        return f"Endpoint={self.event_hubs_endpoint};SharedAccessKeyName={self.shared_access_key_name};SharedAccessKey={self.shared_access_key};EntityPath={self.event_hubs_entity_path}"

    def historian_setup(self):
        conn_str = self.iot_hub_device_connection_string
        self.device_client = IoTHubDeviceClient.create_from_connection_string(
            conn_str
        )
        self.device_client.connect()
        _log.debug(dir(self.device_client))

    def shutdown_iot_hub(self):
        self.device_client.shutdown()


    @Core.receiver("onstop")
    def onstop(self, sender, **kwargs):
        """
        This method is called when the Agent is about to shutdown, but before it disconnects from
        the message bus.
        """
        self.shutdown_iot_hub()


def main():
    """Main method called to start the agent."""
    utils.vip_main(iot_hub_historian, version=__version__)


if __name__ == "__main__":
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
