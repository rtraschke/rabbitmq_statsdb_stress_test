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
-export([run/1, clear_tracer/1, message_queue_len/2]).


% Clear a potentially stuck tracer as set up below in start_handle_cast_tracer/3
-spec clear_tracer([string()]) -> ok.
clear_tracer([Nodename]) when is_list(Nodename) ->
    Node = nodename(Nodename),
    case net_adm:ping(Node) of
        pong ->
            1 = rpc:call(Node, erlang, trace,
                [global:whereis_name(stats_receiver()), false, [call, timestamp]]);
        pang ->
            io:format(standard_error, "Could not connect to ~p.~n", [Node]),
            halt(1)
    end,
    ok.

-spec run([string()]) -> ok.
run([Nodename, CSV]) when is_list(Nodename), is_list(CSV) ->
    Node = nodename(Nodename),
    case net_adm:ping(Node) of
        pong -> io:format("Connected to ~p.~n", [Node]), run_on_node(Node, CSV);
        pang -> io:format(standard_error, "Could not connect to ~p.~n", [Node]), halt(1)
    end.

-spec nodename(string()) -> atom().
nodename(Nodename) ->
    case lists:member($@, Nodename) of
        true ->
            list_to_atom(Nodename);
        false ->
            {ok, Hostname} = inet:gethostname(),
            list_to_atom(Nodename ++ "@" ++ Hostname)
    end.

-spec run_on_node(atom(), string()) -> ok.
run_on_node(Node, CSV) ->
    Stats_Receiver = stats_receiver(),
    Stats_Pid = global:whereis_name(Stats_Receiver),
    io:format("Stats DB is at ~p (~p).~n", [Stats_Receiver, Stats_Pid]),
    {ok, _} = rabbit_mgmt_db_stress_stats:start_link(),
    {ok, Msg_Q_Len} = timer:apply_interval(100, ?MODULE, message_queue_len, [Node, Stats_Pid]),
    start_handle_cast_tracer(Node, Stats_Receiver, Stats_Pid),
    generate_events({global, Stats_Receiver}, CSV),
    stop_handle_cast_tracer(),
    {ok, cancel} = timer:cancel(Msg_Q_Len),
    ok = rabbit_mgmt_db_stress_stats:stop().


-spec message_queue_len(atom(), pid()) -> ok.
message_queue_len(Node, Stats_Pid) ->
    {message_queue_len, Len} = rpc:call(Node, erlang, process_info, [Stats_Pid, message_queue_len]),
    rabbit_mgmt_db_stress_stats:message_queue_len(Len).

-type tracer_state() :: {atom(), atom(), atom(), erlang:timestamp()}.
-spec start_handle_cast_tracer(atom(), atom(), pid()) -> ok.
start_handle_cast_tracer(Node, Stats_Receiver, Stats_Pid) ->
    TS = undefined,
    Event = undefined,
    Tracer_State = {Stats_Receiver, handle_cast, Event, TS},
    {ok, _Tracer} = dbg:tracer(process, {fun handle_cast_tracer/2, Tracer_State}),
    {ok, Node} = dbg:n(Node),
    ok = dbg:cn(node()),
    {ok, [{matched, Node, 1}, {saved, 1}]} = dbg:tp({Stats_Receiver, handle_cast, 2}, [{'_', [], [{return_trace}]}]),
    {ok, [{matched, Node, 1}]} = dbg:p(Stats_Pid, [call, timestamp]),
    ok.

-spec stop_handle_cast_tracer() -> ok.
stop_handle_cast_tracer() ->
    ok = dbg:stop_clear().

-spec handle_cast_tracer(term(), tracer_state()) -> ok.
handle_cast_tracer({trace_ts, _Pid, call,
            {Stats_Receiver, handle_cast, [{event, #event{type = Event}}, _State]},
            TS},
        {Stats_Receiver, handle_cast, undefined, undefined}) ->
    {Stats_Receiver, handle_cast, Event, TS};
handle_cast_tracer({trace_ts, _Pid, return_from,
            {Stats_Receiver, handle_cast, 2},
            _Result,
            TS2},
        {Stats_Receiver, handle_cast, Event, TS1}) ->
    Time_Elapsed = now_to_micros(TS2) - now_to_micros(TS1),
    rabbit_mgmt_db_stress_stats:handle_cast_time(Event, Time_Elapsed),
    {Stats_Receiver, handle_cast, undefined, undefined};
handle_cast_tracer(Msg, State) ->
    io:format(standard_error, "Unexpected handle_call tracer message:~n    ~p~n", [Msg]),
    State.


-record(event_counts, {
    connections :: non_neg_integer(),
    channels_per_connection :: non_neg_integer(),
    queues_per_channel :: non_neg_integer(),
    stats_per_connection :: non_neg_integer(),
    stats_per_channel :: non_neg_integer(),
    stats_per_queue :: non_neg_integer()
}).

-spec generate_events({global, atom()}, string()) -> ok.
generate_events(Stats_DB, CSV) ->
    [ generate_events(Stats_DB, Counts, CSV) || Counts <- event_counts()],
    ok.

-spec event_counts() -> [#event_counts{}].
event_counts() ->
    [
    % 1000 connection_created and connection_closed
        #event_counts{
            connections = 1000, channels_per_connection = 0, queues_per_channel = 0,
            stats_per_connection = 0, stats_per_channel = 0, stats_per_queue = 0
        },
    % 1000000 connection_created and connection_closed
        #event_counts{
            connections = 1000000, channels_per_connection = 0, queues_per_channel = 0,
            stats_per_connection = 0, stats_per_channel = 0, stats_per_queue = 0
        },
    % 1000 channel_created and channel_closed
        #event_counts{
            connections = 1, channels_per_connection = 1000, queues_per_channel = 0,
            stats_per_connection = 0, stats_per_channel = 0, stats_per_queue = 0
        },
    % 1000000 channel_created and channel_closed
        #event_counts{
            connections = 1, channels_per_connection = 1000000, queues_per_channel = 0,
            stats_per_connection = 0, stats_per_channel = 0, stats_per_queue = 0
        },
    % 1000 connection_created and connection_closed
    % 1000000 channel_created and channel_closed
        #event_counts{
            connections = 1000, channels_per_connection = 1000, queues_per_channel = 0,
            stats_per_connection = 0, stats_per_channel = 0, stats_per_queue = 0
        },
    % 1000 connection_stats
        #event_counts{
            connections = 1, channels_per_connection = 1, queues_per_channel = 0,
            stats_per_connection = 1000, stats_per_channel = 0, stats_per_queue = 0
        },
    % 1000000 connection_stats
        #event_counts{
            connections = 1, channels_per_connection = 1, queues_per_channel = 0,
            stats_per_connection = 1000000, stats_per_channel = 0, stats_per_queue = 0
        },
    % 1000 connection_created and connection_closed
    % 1000 channel_created and channel_closed
    % 1000000 connection_stats
        #event_counts{
            connections = 1000, channels_per_connection = 1, queues_per_channel = 0,
            stats_per_connection = 1000, stats_per_channel = 0, stats_per_queue = 0
        },
    % 1000 connection_created and connection_closed
    % 1000000 channel_created and channel_closed
    % 1000000 connection_stats
        #event_counts{
            connections = 1000, channels_per_connection = 1000, queues_per_channel = 0,
            stats_per_connection = 1000, stats_per_channel = 0, stats_per_queue = 0
        }
    ].

-spec generate_events({global, atom()}, #event_counts{}, string()) -> ok.
generate_events(Stats_DB, #event_counts{} = Counts, CSV) ->
    #event_counts{
        connections = N_Conns,
        channels_per_connection = N_Chans,
        stats_per_connection = N_Conn_Stats
    } = Counts,
    ok = rabbit_mgmt_db_stress_stats:reset(),
    Conns = [ pid_and_name(<<"Conn">>, I) || I <- lists:seq(1, N_Conns) ],
    Chans = [ pid_and_name(Conn, I)  || {_, Conn} <- Conns, I <- lists:seq(1, N_Chans) ],
    io:format("Generating ~p connection_created events.~n", [N_Conns]),
    [ gen_server:cast(Stats_DB, connection_created(Pid, Name))
    || {Pid, Name} <- Conns
    ],
    io:format("Generating ~p channel_created events.~n", [N_Conns * N_Chans]),
    [ gen_server:cast(Stats_DB, channel_created(Pid, Name))
    || {Pid, Name} <- Chans
    ],
    io:format("Generating ~p connection_stats events.~n", [N_Conn_Stats * N_Conns]),
    [ gen_server:cast(Stats_DB, connection_stats(Pid, random:uniform(50)))
    || _ <- lists:seq(1, N_Conn_Stats), {Pid, _} <- Conns
    ],
    io:format("Generating ~p channel_closed events.~n", [N_Conns * N_Chans]),
    [ gen_server:cast(Stats_DB, channel_closed(Pid))
    || {Pid, _} <- Chans
    ],
    io:format("Generating ~p connection_closed events.~n", [N_Conns]),
    [ gen_server:cast(Stats_DB, connection_closed(Pid))
    || {Pid, _} <- Conns
    ],
    N_Total_Casts =
        N_Conns +
        N_Conns * N_Chans +
        N_Conns * N_Conn_Stats +
        N_Conns * N_Chans +
        N_Conns,
    io:format("Waiting to receive stats from ~p events.~n", [N_Total_Casts]),
    Stats = rabbit_mgmt_db_stress_stats:get(N_Total_Casts),
    write_to_csv(CSV, Stats),
    io:format("~p~n", [Stats]).


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
        {_, New_Pid} when is_pid(New_Pid) -> rabbit_mgmt_event_collector;
        {Old_Pid, undefined} when is_pid(Old_Pid) -> rabbit_mgmt_db
    end.


-spec pid_and_name(binary(), non_neg_integer()) -> {pid(), binary()}.
pid_and_name(Prefix, I) when is_binary(Prefix), is_integer(I), I >= 0->
    Num = integer_to_binary(I),
    {spawn(fun () -> ok end), <<Prefix/binary, "_", Num/binary>>}.

% The timestamp is in milli seconds
-spec timestamp() -> non_neg_integer().
timestamp() ->
    {Mega, Secs, Micro} = os:timestamp(),
    1000000*1000*Mega + 1000*Secs + (Micro div 1000).

-spec now_to_micros({non_neg_integer(), non_neg_integer(), non_neg_integer()}) -> non_neg_integer().
now_to_micros({Mega, Sec, Micro}) ->
    1000000*1000000*Mega + 1000000*Sec + Micro.


% some code adapted from the test suite

-spec connection_created(pid(), binary()) -> {event, #event{}}.
connection_created(Pid, Name) when is_pid(Pid), is_binary(Name) ->
    event(connection_created, [{pid, Pid}, {name, Name}]).

-spec connection_stats(pid(), non_neg_integer()) -> {event, #event{}}.
connection_stats(Pid, Oct)  when is_pid(Pid), is_integer(Oct), Oct >= 0->
    event(connection_stats, [{pid, Pid}, {recv_oct, Oct}]).

-spec connection_closed(pid()) -> {event, #event{}}.
connection_closed(Pid) when is_pid(Pid) ->
    event(connection_closed, [{pid, Pid}]).

-spec channel_created(pid(), binary()) -> {event, #event{}}.
channel_created(Pid, Name) when is_pid(Pid), is_binary(Name) ->
    event(channel_created, [{pid, Pid}, {name, Name}]).

-spec channel_stats(pid(), list(), list(), list()) -> {event, #event{}}.
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

-spec channel_closed(pid()) -> {event, #event{}}.
channel_closed(Pid) when is_pid(Pid) ->
    event(channel_closed, [{pid, Pid}]).

-spec queue_created(binary()) -> {event, #event{}}.
queue_created(Name) when is_binary(Name) ->
    event(queue_created, [{name, queue(Name)}]).

-spec queue_stats(binary(), non_neg_integer()) -> {event, #event{}}.
queue_stats(Name, Msgs) when is_binary(Name), is_integer(Msgs), Msgs >= 0 ->
    event(queue_stats, [{name, queue(Name)}, {messages, Msgs}]).

-spec queue_deleted(binary()) -> {event, #event{}}.
queue_deleted(Name) when is_binary(Name) ->
    event(queue_deleted, [{name, queue(Name)}]).


-spec event(atom(), list()) -> {event, #event{}}.
event(Type, Props) ->
    {event, #event{
        type = Type,
        props = Props,
        reference = none,
        timestamp = timestamp()
    }}.

-spec queue(binary()) -> #resource{}.
queue(Name) when is_binary(Name)->
    #resource{virtual_host = <<"/">>, kind = queue, name = Name}.

-spec exchange(binary()) -> #resource{}.
exchange(Name) when is_binary(Name)->
    #resource{virtual_host = <<"/">>, kind = exchange, name = Name}.

-spec write_to_csv(string(), [{atom(), [{atom(), number()}]}]) -> ok.
write_to_csv(CSV, Stats) ->
    IoDevice = open_csv(CSV, Stats),
    [file:write(IoDevice, create_line(Stat)) || Stat <- Stats],
    file:close(IoDevice).

-spec open_csv(string(), [{atom(), [{atom(), number()}]}]) -> pid().
open_csv(CSV, Stats) ->
    case file:open(CSV, [write, exclusive]) of
        {error, eexist} ->
            {ok, IoDevice} = file:open(CSV, [append]),
            IoDevice;
        {ok, IoDevice} ->
            file:write(IoDevice, create_header(hd(Stats))),
            IoDevice
    end.

-spec create_line({atom(), [{atom(), number()}]}) -> string().
create_line({Tag, Values}) ->
    Filtered = lists:sort(filter_metrics(Values)),
    format_line(Tag, [V || {_, V} <- Filtered]).

-spec create_header({atom(), [{atom(), number()}]}) -> string().
create_header({_, Values}) ->
    Filtered = lists:sort(filter_metrics(Values)),
    format_line(metric, [K || {K, _} <- Filtered]).

-spec format_line(atom(), [{atom(), number()}]) -> string().
format_line(Tag, Filtered) ->
    io_lib:format("~p~s~n", [Tag, lists:flatten([io_lib:format(",~p",[Value])
                                                 || Value <- Filtered])]).

-spec filter_metrics([{atom(), number()}]) -> [{atom(), number()}].
filter_metrics(Values) ->
    lists:foldl(fun({histogram, _}, Acc) ->
                        Acc;
                   ({percentile, List}, Acc) ->
                        filter_percentiles(List) ++ Acc;
                   (Pair, Acc) ->
                        [Pair | Acc]
                end, [], Values).

-spec filter_percentiles([{atom(), number()}]) -> [{atom(), number()}].
filter_percentiles(List) ->
    lists:map(fun({50, V}) ->
                      {percentile50, V};
                 ({75, V}) ->
                      {percentile75, V};
                 ({90, V}) ->
                      {percentile90, V};
                 ({95, V}) ->
                      {percentile95, V};
                 ({99, V}) ->
                      {percentile99, V};
                 ({999, V}) ->
                      {percentile999, V}
              end, List).
