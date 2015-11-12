%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2015 Pivotal Software, Inc.  All rights reserved.
%%

% Stress test the Mgmt DB by sending it a variety of events

-module(rabbit_mgmt_db_stressor).

-include_lib("rabbit_common/include/rabbit.hrl").

-compile([export_all]).
-export([run/1, message_queue_len/3]).

-record(agg, {
    msg_counts = []
}).


run([Nodename]) when is_list(Nodename) ->
    Node = case lists:member($@, Nodename) of
        true ->
            list_to_atom(Nodename);
        false ->
            {ok, Hostname} = inet:gethostname(),
            list_to_atom(Nodename ++ "@" ++ Hostname)
    end,
    case net_adm:ping(Node) of
        pong -> io:format("Connected to ~p.~n", [Node]), run_on_node(Node);
        pang -> io:format(standard_error, "Could not connect to ~p.~n", [Node]), halt(1)
    end.

run_on_node(Node) ->
    Stats_Receiver = stats_receiver(),
    Stats_Pid = global:whereis_name(Stats_Receiver),
    io:format("Stats DB is at ~p (~p).~n", [Stats_Receiver, Stats_Pid]),
    Aggregator = spawn_link(fun () -> aggregator(#agg{}) end),
    {ok, TRef} = timer:apply_interval(100, ?MODULE, message_queue_len, [Node, Stats_Pid, Aggregator]),
    generate_events({global, Stats_Receiver}),
    {ok, cancel} = timer:cancel(TRef),
    Aggregator ! {get, self()},
    Aggregator ! stop,
    receive
        Stats -> 
            io:format("Message queue length (100ms samples):~n    ~p~n", [Stats])
    end.

message_queue_len(Node, Stats_Pid, Aggregator) ->
    {message_queue_len, _Len} = Aggregator ! rpc:call(Node, erlang, process_info, [Stats_Pid, message_queue_len]).

aggregator(#agg{msg_counts=Msg_Count} = Agg) ->
    receive
        stop ->
            ok;
        {message_queue_len, Len} ->
            aggregator(Agg#agg{msg_counts = [Len | Msg_Count]});
        {get, From} ->
            From ! bear:get_statistics(lists:reverse(Agg#agg.msg_counts)),
            aggregator(Agg);
        _Info ->
            aggregator(Agg)
    end.

generate_events(Stats_DB) ->
    Conns = [ pid_and_name(<<"Conn">>, I) || I <- lists:seq(1, 1000) ],
    Chans = [ pid_and_name(Conn, I)  || {_, Conn} <- Conns, I <- lists:seq(1, 1000) ],
    io:format("Generating ~p connection_created events.~n", [length(Conns)]),
    _Conn_Createds = [ gen_server:cast(Stats_DB, {event, connection_created(Pid, Name)}) || {Pid, Name} <- Conns ],
    io:format("Generating ~p channel_created events.~n", [length(Chans)]),
    _Chan_Createds = [ gen_server:cast(Stats_DB, {event, channel_created(Pid, Name)}) || {Pid, Name} <- Chans ],
    io:format("Generating ~p connection_stats events.~n", [100 * length(Conns)]),
    _Conn_Stats = [ gen_server:cast(Stats_DB, {event, connection_stats(Pid, random:uniform(50))}) || _ <- lists:seq(1, 100), {Pid, _} <- Conns ],
    io:format("Generating ~p channel_closed events.~n", [length(Chans)]),
    _Chan_Closeds = [ gen_server:cast(Stats_DB, {event, channel_closed(Pid)}) || {Pid, _} <- Chans ],
    io:format("Generating ~p connection_closed events.~n", [length(Conns)]),
    _Conn_Closeds = [ gen_server:cast(Stats_DB, {event, connection_closed(Pid)}) || {Pid, _} <- Conns ],
    ok.

-spec pid_and_name(binary(), non_neg_integer()) -> {pid(), binary()}.
pid_and_name(Prefix, I) when is_binary(Prefix), is_integer(I), I >= 0->
    Num = integer_to_binary(I),
    {spawn(fun () -> ok end), <<Prefix/binary, "_", Num/binary>>}.

% Switch easily between old and new versions of the stats db.
-spec stats_receiver() -> 'rabbit_mgmt_event_collector' | 'rabbit_mgmt_db'.
stats_receiver() ->
    stats_receiver(5).

stats_receiver(0) ->
    io:format(standard_error, "Stats DB not found!~n", []),
    halt(1);
stats_receiver(N) ->
    case {global:whereis_name(rabbit_mgmt_db), global:whereis_name(rabbit_mgmt_event_collector)} of
        {undefined, undefined} -> timer:sleep(500), stats_receiver(N-1);
        {undefined, New_Pid} when is_pid(New_Pid) -> rabbit_mgmt_event_collector;
        {Old_Pid, undefined} when is_pid(Old_Pid) -> rabbit_mgmt_db
    end.

-spec timestamp() -> non_neg_integer().
timestamp() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    1000000*1000*MegaSecs + 1000*Secs + (MicroSecs div 1000).


% some code adapted from the test suite

-spec connection_created(pid(), binary()) -> #event{}.
connection_created(Pid, Name) when is_pid(Pid), is_binary(Name) ->
    event(connection_created, [{pid, Pid}, {name, Name}]).

-spec connection_stats(pid(), non_neg_integer()) -> #event{}.
connection_stats(Pid, Oct)  when is_pid(Pid), is_integer(Oct), Oct >= 0->
    event(connection_stats, [{pid, Pid}, {recv_oct, Oct}]).

-spec connection_closed(pid()) -> #event{}.
connection_closed(Pid) when is_pid(Pid) ->
    event(connection_closed, [{pid, Pid}]).

-spec channel_created(pid(), binary()) -> #event{}.
channel_created(Pid, Name) when is_pid(Pid), is_binary(Name) ->
    event(channel_created, [{pid, Pid}, {name, Name}]).

channel_stats(Pid, XStats, QXStats, QStats) ->
    XStats1 = [ {exchange(XName), [{publish, N}]} || {XName, N} <- XStats ],
    QXStats1 = [ {{queue(QName), exchange(XName)}, [{publish, N}]} || {QName, XName, N} <- QXStats ],
    QStats1 = [ {queue(QName), [{deliver_no_ack, N}]} || {QName, N} <- QStats ],
    event(channel_stats, [
        {pid, Pid},
        {channel_exchange_stats, XStats1},
        {channel_queue_exchange_stats, QXStats1},
        {channel_queue_stats, QStats1}
    ]).

-spec channel_closed(pid()) -> #event{}.
channel_closed(Pid) when is_pid(Pid) ->
    event(channel_closed, [{pid, Pid}]).

-spec queue_created(binary()) -> #event{}.
queue_created(Name) when is_binary(Name) ->
    event(queue_created, [{name, queue(Name)}]).

-spec queue_stats(binary(), non_neg_integer()) -> #event{}.
queue_stats(Name, Msgs) when is_binary(Name), is_integer(Msgs), Msgs >= 0 ->
    event(queue_stats, [{name, queue(Name)}, {messages, Msgs}]).

-spec queue_deleted(binary()) -> #event{}.
queue_deleted(Name) when is_binary(Name) ->
    event(queue_deleted, [{name, queue(Name)}]).


-spec event(atom(), list()) -> #event{}.
event(Type, Props) ->
    #event{
        type = Type,
        props = Props,
        reference = none,
        timestamp = timestamp()
    }.

-spec queue(binary()) -> #resource{}.
queue(Name) when is_binary(Name)->
    #resource{virtual_host = <<"/">>, kind = queue, name = Name}.

-spec exchange(binary()) -> #resource{}.
exchange(Name) when is_binary(Name)->
    #resource{virtual_host = <<"/">>, kind = exchange, name = Name}.
