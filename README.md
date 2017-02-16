emqttd_plugin_kafka_bridge
===================

Emqttd kafka bridge Plugin, a bridge transfer messages from emqtt to kafka.

Build Plugin
------------

Add the plugin as submodule of emqttd project.

If the submodules exist:

```
git submodule update --remote plugins/emqttd_plugin_kafka_bridge
```

Orelse:

```
git submodule add https://github.com/Zarathos94/emqttd_kafka_plugin_rev.git plugins/emqttd_plugin_kafka_bridge
```

And then build emqttd project.

Configure Plugin
----------------
TODO: Move broker list to here

File: etc/plugin.config

```erlang
[
  {emqttd_plugin_kafka_bridge, [
  	{kafka, [
      {bootstrap_broker, {"127.0.0.1", 9092} },
      {partition_strategy, strict_round_robin}
    ]}
  ]}
].

```

Broker URL and port setting:
-----------
``bootstrap_broker, {"127.0.0.1", 9092} ``

Partition strategy setting:
-----------
Round robin

``partition_strategy, strict_round_robin``

Random

``partition_strategy, random``


Load Plugin
-----------

```
./bin/emqttd_ctl plugins load emqttd_plugin_kafka_bridge
```

Kafka Topic and messages
-----------

### Topic

All message will be published on to kafka's ``broker_message`` Topic.

### Messages

In the following circumstances, you will receive kafka messages

- when a client connected broker

- when a client disconnected broker

- when a client subscribed a channel

- when a client unsubscribed a channel

- when a client published a message to a channel

- when a client delivered a message

- when a client acknowledged a messages 

All these message will published on to kafka.

