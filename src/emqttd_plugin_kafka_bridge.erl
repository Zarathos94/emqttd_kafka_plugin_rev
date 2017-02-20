%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2016 Huang Rui<vowstar@gmail.com>, All Rights Reserved.
%%%
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

-export([load/1, unload/0]).

%% Hooks functions
-export([on_client_connected/3, on_client_disconnected/3]).

-export([on_client_subscribe/4, on_client_unsubscribe/4]).

-export([on_session_created/3, on_session_subscribed/4, on_session_unsubscribed/4, on_session_terminated/4]).

-export([on_message_publish/2, on_message_delivered/4, on_message_acked/4]).

%% Called when the plugin application start
load(Env) ->
    ekaf_init([Env]),
    emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [Env]),
    emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
    emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
    emqttd:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
    emqttd:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
    emqttd:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
    emqttd:hook('session.terminated', fun ?MODULE:on_session_terminated/4, [Env]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqttd:hook('message.delivered', fun ?MODULE:on_message_delivered/4, [Env]),
    emqttd:hook('message.acked', fun ?MODULE:on_message_acked/4, [Env]).

%%-----------client connect start-----------------------------------%%

on_client_connected(ConnAck, Client = #mqtt_client{client_id  = ClientId}, _Env) ->
    %io:format("client ~s connected, connack: ~w~n", [ClientId, ConnAck]),

    Json = mochijson2:encode([
        {type, <<"connected">>},
        {client_id, ClientId},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),

    ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),

    {ok, Client}.

%%-----------client connect end-------------------------------------%%



%%-----------client disconnect start---------------------------------%%

on_client_disconnected(Reason, _Client = #mqtt_client{client_id = ClientId}, _Env) ->
    %io:format("client ~s disconnected, reason: ~w~n", [ClientId, Reason]),

    Json = mochijson2:encode([
        {type, <<"disconnected">>},
        {client_id, ClientId},
        {reason, Reason},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),

    ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),

    ok.

%%-----------client disconnect end-----------------------------------%%



%%-----------client subscribed start---------------------------------------%%

%% should retain TopicTable
on_client_subscribe(ClientId, Username, TopicTable, _Env) ->
    %io:format("client ~s subscribed ~p~n", [ClientId, TopicTable]),
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
                {ts, emqttd_time:now_to_secs()}
            ]),
            ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json));
        _ ->
            %% If TopicTable is empty
            io:format("empty topic ~n")
    end,

    {ok, TopicTable}.
%%-----------client subscribed end----------------------------------------%%



%%-----------client unsubscribed start----------------------------------------%%

on_client_unsubscribe(ClientId, Username, TopicTable, _Env) ->
    %io:format("client ~s(~s) unsubscribe ~p~n", [ClientId, Username, TopicTable]),
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
                {ts, emqttd_time:now_to_secs()}
            ]),
            ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json));
        _ ->
            %% If TopicTable is empty
            io:format("empty topic ~n")
    end,

    {ok, TopicTable}.


%%-----------message publish start--------------------------------------%%

%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    %io:format("publish ~s~n", [emqttd_message:format(Message)]),

    {ClientId, Username} = Message#mqtt_message.from,
    %Sender =  Message#mqtt_message.sender,
    Topic = Message#mqtt_message.topic,
    Payload = Message#mqtt_message.payload,
    QoS = Message#mqtt_message.qos,
    Timestamp = Message#mqtt_message.timestamp,

    Json = mochijson2:encode([
        {type, <<"published">>},
        {client_id, ClientId},
        {username, Username},
        {topic, Topic},
        {payload, Payload},
        {qos, QoS},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),

    ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),

    {ok, Message}.

on_session_created(ClientId, Username, _Env) ->
    %io:format("session(~s/~s) created.", [ClientId, Username]),
    Json = mochijson2:encode([
        {type, <<"session_created">>},
        {client_id, ClientId},
        {username, Username},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),
    ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)).

on_session_subscribed(ClientId, Username, {Topic, Opts}, _Env) ->
    %io:format("session(~s/~s) subscribed: ~p~n", [Username, ClientId, {Topic, Opts}]),
    Json = mochijson2:encode([
        {type, <<"session_created">>},
        {client_id, ClientId},
        {username, Username},
        {topic, Topic},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),
    ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),
    {ok, {Topic, Opts}}.

on_session_unsubscribed(ClientId, Username, {Topic, Opts}, _Env) ->
    %io:format("session(~s/~s) unsubscribed: ~p~n", [Username, ClientId, {Topic, Opts}]),
    Json = mochijson2:encode([
        {type, <<"session_unsubscribed">>},
        {client_id, ClientId},
        {username, Username},
        {topic, Topic},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),
    ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),
    ok.

on_session_terminated(ClientId, Username, Reason, _Env) ->
    %io:format("session(~s/~s) terminated: ~p.", [ClientId, Username, Reason]),
    Json = mochijson2:encode([
        {type, <<"session_terminated">>},
        {client_id, ClientId},
        {username, Username},
        {reason, Reason},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),
    ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)).


%%-----------message delivered start--------------------------------------%%
on_message_delivered(ClientId, Username, Message, _Env) ->
    %io:format("delivered to client ~s: ~s~n", [ClientId, emqttd_message:format(Message)]),

    {SenderId, SenderName} = Message#mqtt_message.from,
    %Sender =  Message#mqtt_message.sender,
    Topic = Message#mqtt_message.topic,
    Payload = Message#mqtt_message.payload,
    QoS = Message#mqtt_message.qos,
    Timestamp = Message#mqtt_message.timestamp,

    Json = mochijson2:encode([
        {type, <<"delivered">>},
        {client_id, ClientId},
        {sender_id, SenderId},
        {sender_name, SenderName},
        {username, Username},
        {topic, Topic},
        {payload, Payload},
        {qos, QoS},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),

    ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),

    {ok, Message}.
%%-----------message delivered end----------------------------------------%%

%%-----------acknowledgement publish start----------------------------%%
on_message_acked(ClientId, Username, Message, _Env) ->
    %io:format("client ~s acked: ~s~n", [ClientId, emqttd_message:format(Message)]),

    {SenderId, SenderName} = Message#mqtt_message.from,
    %Sender =  Message#mqtt_message.sender,
    Topic = Message#mqtt_message.topic,
    Payload = Message#mqtt_message.payload,
    QoS = Message#mqtt_message.qos,
    Timestamp = Message#mqtt_message.timestamp,

    Json = mochijson2:encode([
        {type, <<"acked">>},
        {client_id, ClientId},
        {username, Username},
        {sender_id, SenderId},
        {sender_name, SenderName},
        {topic, Topic},
        {payload, Payload},
        {qos, QoS},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),

    ekaf:produce_async_batched(<<"broker_message">>, list_to_binary(Json)),
    {ok, Message}.

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

    io:format("Initialized ekaf with ~p~n | for topic 'broker_message'", [BootstrapBroker]).


%% Called when the plugin application stop
unload() ->
    emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3),
    emqttd:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqttd:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
    emqttd:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4),
    emqttd:unhook('session.created', fun ?MODULE:on_session_created/3),
    emqttd:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    emqttd:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
    emqttd:unhook('session.terminated', fun ?MODULE:on_session_terminated/4),
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqttd:unhook('message.delivered', fun ?MODULE:on_message_delivered/4),
    emqttd:unhook('message.acked', fun ?MODULE:on_message_acked/4).
