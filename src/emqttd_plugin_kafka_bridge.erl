%%%-----------------------------------------------------------------------------
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd_plugin_kafka_bridge.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_plugin_kafka_bridge).

-include("../../emqttd/include/emqttd.hrl").

-include("../../emqttd/include/emqttd_protocol.hrl").

-include("../../emqttd/include/emqttd_internal.hrl").

-include("../../amqp_client/include/amqp_client.hrl").
-define(APP, emqttd_plugin_kafka_bridge).

-export([load/0, unload/0]).

%% Hooks functions
-export([on_client_connected/3, on_client_disconnected/3]).

-export([on_client_subscribe/4, on_client_unsubscribe/4]).

-export([on_session_created/3, on_session_subscribed/4, on_session_unsubscribed/4, on_session_terminated/4]).

-export([on_message_publish/2, on_message_delivered/4, on_message_acked/4]).

-export([uuid_to_string/1]).

-export([foreach/2]).

%% Called when the plugin application start
load() ->
    %ekaf_init([Env]),
    rmq_init(),
    emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [application:get_env(?APP)]),
    emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [application:get_env(?APP)]),
    emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [application:get_env(?APP)]),
    emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [application:get_env(?APP)]),
    emqttd:hook('session.created', fun ?MODULE:on_session_created/3, [application:get_env(?APP)]),
    emqttd:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [application:get_env(?APP)]),
    emqttd:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [application:get_env(?APP)]),
    emqttd:hook('session.terminated', fun ?MODULE:on_session_terminated/4, [application:get_env(?APP)]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [application:get_env(?APP)]),
    emqttd:hook('message.delivered', fun ?MODULE:on_message_delivered/4, [application:get_env(?APP)]),
    emqttd:hook('message.acked', fun ?MODULE:on_message_acked/4, [application:get_env(?APP)]).



uuid_to_string(<<I:128>>) ->
  integer_to_list(I, 32).

%% ---------- Cluster support -------------------------------------%%

foreach(F, [H|T]) ->
    F(H),
    foreach(F, T);
foreach(F, []) ->
    ok.
%%-----------client connect start-----------------------------------%%

on_client_connected(ConnAck, Client = #mqtt_client{client_id  = ClientId}, _Env) ->

    Json = mochijson2:encode([
        {type, <<"connected">>},
        {client_id, ClientId},
        {cluster_node, node()},
        {timestamp, erlang:system_time(micro_seconds)}
    ]),
    {ok, Channel3} = application:get_env(?APP, rmq_channel3),
    Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = <<"emqttd_connected">>},
    amqp_channel:cast(Channel3, Publish, #amqp_msg{payload = list_to_binary(Json)}),
    %ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),

    {ok, Client}.

%%-----------client connect end-------------------------------------%%



%%-----------client disconnect start---------------------------------%%

on_client_disconnected(Reason, _Client = #mqtt_client{client_id = ClientId}, _Env) ->

    Json = mochijson2:encode([
        {type, <<"disconnected">>},
        {client_id, ClientId},
        {reason, Reason},
        {cluster_node, node()},
        {timestamp, erlang:system_time(micro_seconds)}
    ]),
    {ok, Channel3} = application:get_env(?APP, rmq_channel3),
    Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = <<"emqttd_disconnected">>},
    amqp_channel:cast(Channel3, Publish, #amqp_msg{payload = list_to_binary(Json)}),
    %ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),

    ok.

%%-----------client disconnect end-----------------------------------%%



%%-----------client subscribed start---------------------------------------%%

%% should retain TopicTable
on_client_subscribe(ClientId, Username, TopicTable, _Env) ->
    case TopicTable of
        [_|_] ->
            %% If TopicTable list is not empty
            Key = proplists:get_keys(TopicTable),
            %% build json to send using ClientId
            Json = mochijson2:encode([
                {type, <<"subscribed">>},
                {client_id, ClientId},
                {username, Username},
                {topic, lists:last(Key)},
                {cluster_node, node()},
                {timestamp, erlang:system_time(micro_seconds)}
            ]),
            {ok, Channel} = application:get_env(?APP, rmq_channel),
            Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = <<"emqttd_subscriptions">>},
            amqp_channel:cast(Channel, Publish, #amqp_msg{payload = list_to_binary(Json)});
            %ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json));
        _ ->
            %% If TopicTable is empty
            io:format("empty topic ~n")
    end,

    {ok, TopicTable}.
%%-----------client subscribed end----------------------------------------%%



%%-----------client unsubscribed start----------------------------------------%%

on_client_unsubscribe(ClientId, Username, TopicTable, _Env) ->
    case TopicTable of
        [_|_] ->
            %% If TopicTable list is not empty
            Key = proplists:get_keys(TopicTable),
            %% build json to send using ClientId
            Json = mochijson2:encode([
                {type, <<"unsubscribed">>},
                {client_id, ClientId},
                {username, Username},
                {topic, lists:last(Key)},
                {cluster_node, node()},
                {timestamp, erlang:system_time(micro_seconds)}
            ]),
            {ok, Channel} = application:get_env(?APP, rmq_channel),
            Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = <<"emqttd_unsubscriptions">>},
            amqp_channel:cast(Channel, Publish, #amqp_msg{payload = list_to_binary(Json)});
            %ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json));
        _ ->
            %% If TopicTable is empty
            io:format("empty topic ~n")
    end,

    {ok, TopicTable}.


%%-----------message publish start--------------------------------------%%

%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #mqtt_message{topic = <<"symbol/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #mqtt_message{topic = <<"symbols/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #mqtt_message{topic = <<"chat/", _/binary>>}, _Env) ->
    {ClientId, Username} = Message#mqtt_message.from,
    MessageId = Message#mqtt_message.id,
    Topic = Message#mqtt_message.topic,
    Payload = Message#mqtt_message.payload,
    {ok, RMQRoutes} = application:get_env(?APP, routing_config),
    Index = string:str(RMQRoutes, lists:last(string:tokens(binary_to_list(Topic), "/"))),
    if Index > 0 ->
            Json = mochijson2:encode([
                {type, <<"chat_event">>},
                {client_id, ClientId},
                {username, Username},
                {topic, Topic},
                {payload, Payload},
                {message_id, emqttd_guid:to_hexstr(MessageId)},
                {cluster_node, node()},
                {timestamp, erlang:system_time(micro_seconds)}
            ]),
            {ok, Channel1} = application:get_env(?APP, rmq_channel1),
            Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = list_to_binary(lists:last(string:tokens(binary_to_list(Topic), "/")))},
            amqp_channel:cast(Channel1, Publish, #amqp_msg{payload = list_to_binary(Json)}),
            {ok, Message};
        true ->
            {ok, Message}
    end;
    

on_message_publish(Message = #mqtt_message{topic = <<"thread/", _/binary>>}, _Env) ->

    {ClientId, Username} = Message#mqtt_message.from,
    MessageId = Message#mqtt_message.id,
    Topic = Message#mqtt_message.topic,
    Payload = Message#mqtt_message.payload,
    {ok, RMQRoutes} = application:get_env(?APP, routing_config),
    Index = string:str(RMQRoutes, lists:last(string:tokens(binary_to_list(Topic), "/"))),
    if Index > 0 ->
                Json = mochijson2:encode([
                    {type, <<"chat_event">>},
                    {client_id, ClientId},
                    {username, Username},
                    {topic, Topic},
                    {payload, Payload},
                    {message_id, emqttd_guid:to_hexstr(MessageId)},
                    {cluster_node, node()},
                    {timestamp, erlang:system_time(micro_seconds)}
                ]),
                {ok, Channel1} = application:get_env(?APP, rmq_channel1),
                Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = list_to_binary(lists:last(string:tokens(binary_to_list(Topic), "/")))},
                amqp_channel:cast(Channel1, Publish, #amqp_msg{payload = list_to_binary(Json)}),
                {ok, Message};
            true ->
                {ok, Message}
        end;

on_message_publish(Message, _Env) ->
    {ok, Message}.
    
on_message_publish(Message = #mqtt_message{topic = <<"event_tracking/", _/binary>>}, _Env) ->
        {ClientId, Username} = Message#mqtt_message.from,
        MessageId = Message#mqtt_message.id,
        Topic = Message#mqtt_message.topic,
        Payload = Message#mqtt_message.payload,
    
        Json = mochijson2:encode([
            {type, <<"event_published">>},
            {client_id, ClientId},
            {username, Username},
            {topic, Topic},
            {payload, Payload},
            {message_id, emqttd_guid:to_hexstr(MessageId)},
            {cluster_node, node()},
            {timestamp, erlang:system_time(micro_seconds)}
        ]),
    
        {ok, Channel2} = application:get_env(?APP, rmq_channel2),
        Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = <<"event_log_route">>},
        amqp_channel:cast(Channel2, Publish, #amqp_msg{payload = list_to_binary(Json)}),
        {ok, Message}..


on_session_created(ClientId, Username, _Env) ->
    Json = mochijson2:encode([
        {type, <<"session_created">>},
        {client_id, ClientId},
        {username, Username},
        {cluster_node, node()},
        {timestamp, erlang:system_time(micro_seconds)}
    ]),
    {ok, Channel} = application:get_env(?APP, rmq_channel),
    Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = <<"emqttd_session_created">>},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = list_to_binary(Json)}).
    %ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)).

on_session_subscribed(ClientId, Username, {Topic, Opts}, _Env) ->
    Json = mochijson2:encode([
        {type, <<"session_subscribed">>},
        {client_id, ClientId},
        {username, Username},
        {topic, Topic},
        {cluster_node, node()},
        {timestamp, erlang:system_time(micro_seconds)}
    ]),
    {ok, Channel} = application:get_env(?APP, rmq_channel),
    Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = <<"emqttd_session_subscriptions">>},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = list_to_binary(Json)}),
    %ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),
    {ok, {Topic, Opts}}.

on_session_unsubscribed(ClientId, Username, {Topic, Opts}, _Env) ->
    Json = mochijson2:encode([
        {type, <<"session_unsubscribed">>},
        {client_id, ClientId},
        {username, Username},
        {topic, Topic},
        {cluster_node, node()},
        {timestamp, erlang:system_time(micro_seconds)}
    ]),
    {ok, Channel} = application:get_env(?APP, rmq_channel),
    Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = <<"emqttd_session_unsubscriptions">>},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = list_to_binary(Json)}),
    %ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),
    ok.

on_session_terminated(ClientId, Username, Reason, _Env) ->
    Json = mochijson2:encode([
        {type, <<"session_terminated">>},
        {client_id, ClientId},
        {username, Username},
        {reason, Reason},
        {cluster_node, node()},
        {timestamp, erlang:system_time(micro_seconds)}
    ]),
    {ok, Channel} = application:get_env(?APP, rmq_channel),
    Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = <<"emqttd_session_termination">>},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = list_to_binary(Json)}).
    %ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)).


%%-----------message delivered start--------------------------------------%%
on_message_delivered(ClientId, Username, Message, _Env) ->

    {SenderId, SenderName} = Message#mqtt_message.from,
    Topic = Message#mqtt_message.topic,
    MessageId = Message#mqtt_message.id,
    Payload = Message#mqtt_message.payload,

    Json = mochijson2:encode([
        {type, <<"message_delivered">>},
        {client_id, ClientId},
        {sender_id, SenderId},
        {sender_name, SenderName},
        {username, Username},
        {topic, Topic},
        {payload, Payload},
        {message_id, emqttd_guid:to_hexstr(MessageId)},
        {cluster_node, node()},
        {timestamp, erlang:system_time(micro_seconds)}
    ]),
    {ok, Channel} = application:get_env(?APP, rmq_channel),
    Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = <<"emqttd_delivery_report">>},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = list_to_binary(Json)}),
    %ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),

    {ok, Message}.
%%-----------message delivered end----------------------------------------%%

%%-----------acknowledgement publish start----------------------------%%
on_message_acked(ClientId, Username, Message, _Env) ->

    {SenderId, SenderName} = Message#mqtt_message.from,
    Topic = Message#mqtt_message.topic,
    Payload = Message#mqtt_message.payload,

    Json = mochijson2:encode([
        {type, <<"message_acked">>},
        {client_id, ClientId},
        {username, Username},
        {sender_id, SenderId},
        {sender_name, SenderName},
        {topic, Topic},
        {payload, Payload},
        %{qos, QoS},
        {cluster_node, node()}
        %{ts, Timestamp}
    ]),

    {ok, Channel} = application:get_env(?APP, rmq_channel),

    Publish = #'basic.publish'{exchange = <<"emqttd">>, routing_key = <<"emqttd_ack_report">>},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = list_to_binary(Json)}),
    %ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),
    {ok, Message}.

%% ===================================================================
%% rmq_init
%% ===================================================================

rmq_init() ->
  {ok, Rmq} = application:get_env(?APP, server),
  {ok, Virtualhost} = application:get_env(?APP, virtualhost),
  {ok, Username} = application:get_env(?APP, username),
  {ok, Password} = application:get_env(?APP, password),
  {ok, RMQPort} = application:get_env(?APP, port),
  {ok, RMQHost} = application:get_env(?APP, host),
  {ok, RMQRoutes} = application:get_env(?APP, routing_config),
  io:format("Trying to connect to:  ~p~n", [RMQHost]),
  
  {ok, Connection} = amqp_connection:start(#amqp_params_network{
    username = list_to_binary(Username), password = list_to_binary(Password), virtual_host = list_to_binary(Virtualhost),
    host = RMQHost, port = RMQPort,
    frame_max = 0, heartbeat = 10, connection_timeout = infinity,
    ssl_options = none, auth_mechanisms = [fun amqp_auth_mechanisms:plain/3, fun amqp_auth_mechanisms:amqplain/3],
    client_properties = [], socket_options = []
  }),
  {ok, Channel} = amqp_connection:open_channel(Connection),
  application:set_env(?APP, rmq_channel, Channel),

  {ok, Channel1} = amqp_connection:open_channel(Connection),
  application:set_env(?APP, rmq_channel1, Channel1),

  {ok, Channel2} = amqp_connection:open_channel(Connection),
  application:set_env(?APP, rmq_channel2, Channel2),

  {ok, Channel3} = amqp_connection:open_channel(Connection),
  application:set_env(?APP, rmq_channel3, Channel3),

  DeclareExchange = #'exchange.declare'{exchange = <<"emqttd">>},
  #'exchange.declare_ok'{} = amqp_channel:call(Channel, DeclareExchange),

    foreach(fun(H) -> 
        DeclareQueueList = #'queue.declare'{queue = list_to_binary(lists:last(string:tokens(H, ".")))},
        #'queue.declare_ok'{} = amqp_channel:call(Channel, DeclareQueueList),
        BindingQueueList = #'queue.bind'{queue       = list_to_binary(lists:last(string:tokens(H, "."))),
        exchange    = <<"emqttd">>,
        routing_key = list_to_binary(lists:last(lists:reverse(string:tokens(H, "."))))},
        #'queue.bind_ok'{} = amqp_channel:call(Channel, BindingQueueList),
    io:format("Binding route : ~p to ~p ~n", [lists:last(string:tokens(H, ".")), lists:last(lists:reverse(string:tokens(H, ".")))])
    end,
    string:tokens(RMQRoutes, ",")),



  %% ============================== Queue bindings and declarations ========================
  DeclareQueueConnected = #'queue.declare'{queue = <<"connected">>},
  #'queue.declare_ok'{} = amqp_channel:call(Channel, DeclareQueueConnected),
  BindingConnected = #'queue.bind'{queue       = <<"connected">>,
                             exchange    = <<"emqttd">>,
                             routing_key = <<"emqttd_connected">>},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, BindingConnected),


  DeclareQueueDisconnected = #'queue.declare'{queue = <<"disconnected">>},
  #'queue.declare_ok'{} = amqp_channel:call(Channel, DeclareQueueDisconnected),

  BindingDisconnected = #'queue.bind'{queue       = <<"disconnected">>,
                                exchange    = <<"emqttd">>,
                                routing_key = <<"emqttd_disconnected">>},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, BindingDisconnected),


  EventQueueDeclare = #'queue.declare'{queue = <<"event_log">>},
  #'queue.declare_ok'{} = amqp_channel:call(Channel, EventQueueDeclare),

  BindingEventPublish = #'queue.bind'{queue       = <<"event_log">>,
                                exchange    = <<"emqttd">>,
                                routing_key = <<"event_log_route">>},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, BindingEventPublish),


  DeclareQueueSessionCreated = #'queue.declare'{queue = <<"session_created">>},
  #'queue.declare_ok'{} = amqp_channel:call(Channel, DeclareQueueSessionCreated),

  BindingSessionCreated = #'queue.bind'{queue       = <<"session_created">>,
                                exchange    = <<"emqttd">>,
                                routing_key = <<"emqttd_session_created">>},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, BindingSessionCreated),



  DeclareQueueSessionSubscriptions = #'queue.declare'{queue = <<"session_subscriptions">>},
  #'queue.declare_ok'{} = amqp_channel:call(Channel, DeclareQueueSessionSubscriptions),

  BindingSessionSubscriptions = #'queue.bind'{queue       = <<"session_subscriptions">>,
                                exchange    = <<"emqttd">>,
                                routing_key = <<"emqttd_session_subscriptions">>},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, BindingSessionSubscriptions),


  DeclareQueueSessionUnSubscriptions = #'queue.declare'{queue = <<"session_unsubscriptions">>},
  #'queue.declare_ok'{} = amqp_channel:call(Channel, DeclareQueueSessionUnSubscriptions),

  BindingSessionUnSubscriptions = #'queue.bind'{queue       = <<"session_unsubscriptions">>,
                                exchange    = <<"emqttd">>,
                                routing_key = <<"emqttd_session_unsubscriptions">>},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, BindingSessionUnSubscriptions),


  DeclareQueueSessionTermination = #'queue.declare'{queue = <<"session_termination">>},
  #'queue.declare_ok'{} = amqp_channel:call(Channel, DeclareQueueSessionTermination),

  BindingSessionTermination = #'queue.bind'{queue       = <<"session_termination">>,
                                exchange    = <<"emqttd">>,
                                routing_key = <<"emqttd_session_termination">>},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, BindingSessionTermination),

  DeclareQueueDeliveryReport = #'queue.declare'{queue = <<"delivery_report">>},
  #'queue.declare_ok'{} = amqp_channel:call(Channel, DeclareQueueDeliveryReport),

  BindingDeliveryReport = #'queue.bind'{queue       = <<"delivery_report">>,
                                exchange    = <<"emqttd">>,
                                routing_key = <<"emqttd_delivery_report">>},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, BindingDeliveryReport),

  DeclareQueueAckReport = #'queue.declare'{queue = <<"ack_report">>},
  #'queue.declare_ok'{} = amqp_channel:call(Channel, DeclareQueueAckReport),

  BindingAckReport = #'queue.bind'{queue       = <<"ack_report">>,
                                exchange    = <<"emqttd">>,
                                routing_key = <<"emqttd_ack_report">>},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, BindingAckReport),


  {ok, _} = application:ensure_all_started(amqp_client),
  io:format("Initialized rabbitmq connection to host: ~p:~p with exchange: ~p on channel: ~p~n", [RMQHost, RMQPort, "emqttd", Channel]).

%% ===================================================================
%% ekaf_init
%% ===================================================================

ekaf_init(_Env) ->
    %% Get parameters
    {ok, Kafka} = application:get_env(emqttd_plugin_kafka_bridge, kafka),
    BootstrapBroker = proplists:get_value(bootstrap_broker, Kafka),
    PartitionStrategy= proplists:get_value(partition_strategy, Kafka),
    %% Set partition strategy, like application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
    application:set_env(ekaf, ekaf_partition_strategy, PartitionStrategy),
    %% Set broker url and port, like application:set_env(ekaf, ekaf_bootstrap_broker, {"127.0.0.1", 9092}),
    application:set_env(ekaf, ekaf_bootstrap_broker, BootstrapBroker),

    %% Set topic
    application:set_env(ekaf, ekaf_bootstrap_topics, <<"broker_message">>),

    {ok, _} = application:ensure_all_started(kafkamocker),
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(ranch),
    {ok, _} = application:ensure_all_started(ekaf),

    io:format("Initialized ekaf with ~p~n", [BootstrapBroker]).


%% Called when the plugin application stop
unload() ->
    emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3),
    emqttd:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqttd:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
    emqttd:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4),
    %emqttd:unhook('session.created', fun ?MODULE:on_session_created/3),
    emqttd:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    emqttd:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
    %emqttd:unhook('session.terminated', fun ?MODULE:on_session_terminated/4),
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqttd:unhook('message.delivered', fun ?MODULE:on_message_delivered/4),
    emqttd:unhook('message.acked', fun ?MODULE:on_message_acked/4).
