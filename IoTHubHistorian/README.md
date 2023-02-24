# IoT Hub Historian
### Created by ACE IoT Solutions LLC

## Functionality
The IoT Hub Historian utilizes the VOLTTRON Historian Framework to collect data from a local VOLTTRON Platform and publish it to a configured Azure IoT Hub Device Endpoint.

The Historian expects to be configured with the following configuration format.


```
{
  # VOLTTRON config files are JSON with support for python style comments.
  "iot_hub_device_connection_string": ""
}
```

## Developing
The Historian was developed against Volttron 7.x and Python 3.8.5
Pytest was used as the testing framework
The tests expect the following environment variables to be set:

```
IOT_HUB_DEVICE_CONNECTION_STRING
```
Values should be analogous to the agent config, it's recommended to provision a specific EventHubs topic for testing, to avoid conflicts.
