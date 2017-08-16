-module (emqttd_plugin_kafka_bridge_config).

-define(APP, emqttd_plugin_kafka_bridge).

-export ([register/0, unregister/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
register() ->
    clique_config:load_schema([code:priv_dir(?APP)], ?APP),
    register_config().

unregister() ->
    unregister_config(),
    clique_config:unload_schema(?APP).

%%--------------------------------------------------------------------
%% Set ENV Register Config
%%--------------------------------------------------------------------
register_config() ->
    Keys = keys(),
    [clique:register_config(Key , fun config_callback/2) || Key <- Keys],
    clique:register_config_whitelist(Keys, ?APP).

config_callback([_, _, Key], Value) ->
    application:set_env(?APP, list_to_atom(Key), Value),
    " successfully\n".

%%--------------------------------------------------------------------
%% UnRegister config
%%--------------------------------------------------------------------
unregister_config() ->
    Keys = keys(),
    [clique:unregister_config(Key) || Key <- Keys],
    clique:unregister_config_whitelist(Keys, ?APP).

keys() ->
    [
    "emqttd_plugin_kafka_bridge.amqp_client.username", 
    "emqttd_plugin_kafka_bridge.amqp_client.password", 
    "emqttd_plugin_kafka_bridge.amqp_client.virtualhost",
    "emqttd_plugin_kafka_bridge.amqp_client.host", 
    "emqttd_plugin_kafka_bridge.amqp_client.port"
    ].