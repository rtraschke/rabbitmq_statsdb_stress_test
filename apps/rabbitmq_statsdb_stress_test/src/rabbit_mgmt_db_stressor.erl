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
run([Nodename, CSV, Option]) when is_list(Nodename), is_list(CSV), is_list(Option) ->
    Node = nodename(Nodename),
    case net_adm:ping(Node) of
        pong -> io:format("Connected to ~p.~n", [Node]),
                run_on_node(Node, CSV, list_to_integer(Option));
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

-spec run_on_node(atom(), string(), integer()) -> ok.
run_on_node(Node, CSV, Option) ->
    Stats_Receiver = stats_receiver(),
    Stats_Pid = global:whereis_name(Stats_Receiver),
    io:format("Stats DB is at ~p (~p).~n", [Stats_Receiver, Stats_Pid]),
    {ok, _} = rabbit_mgmt_db_stress_stats:start_link(),
%    {ok, Tokens, _} = erl_scan:string("fun(_) -> {ok, ignore_this} end."),
%    {ok, Ast} = erl_parse:parse_exprs(Tokens),
%    {value, Fun, _} = rpc:call(Node, erl_eval, exprs, [Ast, []]),
%    ok = rpc:call(Node, Stats_Receiver, override_lookups, [[{exchange, Fun}, {queue, Fun}]]),
    {ok, Msg_Q_Len} = timer:apply_interval(100, ?MODULE, message_queue_len, [Node, Stats_Pid]),
    start_handle_cast_tracer(Node, Stats_Receiver, Stats_Pid),
    Mgmt_DB_Caller = spawn_link(fun () -> mgmt_db_call_loop(Node) end),
    generate_events({global, Stats_Receiver}, Mgmt_DB_Caller, CSV, Option),
    Mgmt_DB_Caller ! stop,
    stop_handle_cast_tracer(),
    {ok, cancel} = timer:cancel(Msg_Q_Len),
%    ok = rpc:call(Node, Stats_Receiver, reset_lookups, []),
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

-spec mgmt_db_call_loop(atom()) -> no_return().
mgmt_db_call_loop(Node) ->
    Default_Calls = [{overview, true}, {all_connections, true}, {all_channels, true}, {all_consumers, true}],
    mgmt_db_call_loop(Node, Default_Calls).

-spec mgmt_db_call_loop(atom(), [{atom(), boolean() | list()}]) -> no_return().
mgmt_db_call_loop(Node, Call_Types) ->
    New_Call_Types = receive
        stop ->
            exit(normal);
        {Type, On_Off} when is_boolean(On_Off) ->
            lists:keystore(Type, 1, Call_Types, {Type, On_Off});
        {Type, []} ->
            lists:keydelete(Type, 1, Call_Types);
        {Type, Objects} ->
            lists:keystore(Type, 1, Call_Types, {Type, Objects})
    after 0 ->
        Call_Types
    end,
    mgmt_db_call(Node, hd(New_Call_Types)),
    timer:sleep(100),
    mgmt_db_call_loop(Node, rotate(New_Call_Types)).

-spec rotate(list()) -> list().
rotate([H | T]) -> T ++ [H].

-spec mgmt_db_call(atom(), {atom(), list()}) -> ok.
mgmt_db_call(Node, {overview, true}) ->
    record_call_time(Node, rabbit_mgmt_db, get_overview, [ranges()]);
mgmt_db_call(_Node, {overview, false}) ->
    ok;
mgmt_db_call(Node, {all_connections, true}) ->
    case random:uniform(4) of
        1 -> record_call_time(Node, rabbit_mgmt_db, get_all_connections, [ranges()]);
        _ -> ok
    end;
mgmt_db_call(_Node, {all_connections, false}) ->
    ok;
mgmt_db_call(Node, {all_channels, true}) ->
    case random:uniform(4) of
        1 -> record_call_time(Node, rabbit_mgmt_db, get_all_channels, [ranges()]);
        _ -> ok
    end;
mgmt_db_call(_Node, {all_channels, false}) ->
    ok;
mgmt_db_call(Node, {all_consumers, true}) ->
    record_call_time(Node, rabbit_mgmt_db, get_all_consumers, []);
mgmt_db_call(_Node, {all_consumers, false}) ->
    ok;
mgmt_db_call(Node, {vhosts, VHosts}) ->
    record_call_time(Node, rabbit_mgmt_db, augment_vhosts, [VHosts, ranges()]);
mgmt_db_call(_Node, {nodes, _Nodes}) ->
    ok;
mgmt_db_call(Node, {connections, Conns}) ->
    N = length(Conns),
    [ record_call_time(Node, rabbit_mgmt_db, get_connection, [Name, ranges()])
    || Name <- Conns, random:uniform(N) < 10
    ];
mgmt_db_call(Node, {channels, Chans}) ->
    N = length(Chans),
    [ record_call_time(Node, rabbit_mgmt_db, get_channel, [Name, ranges()])
    || Name <- Chans, random:uniform(N) < 10
    ];
mgmt_db_call(_Node, {exchanges, _Exchanges}) ->
    ok;
mgmt_db_call(_Node, {queues, _Queues}) ->
    ok.

-spec record_call_time(atom(), atom(), atom(), list()) -> ok.
record_call_time(Node, M, F, A) ->
    {Time_Elapsed, _Result} = rpc:call(Node, timer, tc, [M, F, A]),
    rabbit_mgmt_db_stress_stats:handle_call_time(F, Time_Elapsed).

% Mgmt UI default: 60 second ranges in 5 second increments (rounded down)
% For the stress test, use 5 second ranges in 1 second increments without rounding.
% Copied from rabbitmq-management/include/rabbit_mgmt.hrl :
-record(range, {first, last, incr}).
% Copied mostly from rabbitmq-management/src/rabbit_mgmt_util.erl :
-spec ranges() -> {#range{}, #range{}, #range{}, #range{}}.
ranges() ->
    Age = 5 * 1000, % default: 60 * 1000
    Incr = 1 * 1000, % default: 5 * 1000
    Now = timestamp(),
    Last = Now, % default: (Now div Incr) * Incr
    R = #range{first = (Last - Age), last  = Last, incr  = Incr},
    {R, R, R, R}.


-record(event_counts, {
    connections = 0 :: non_neg_integer(),
    channels_per_connection = 0 :: non_neg_integer(),
    queues_per_channel = 0 :: non_neg_integer(),
    stats_per_connection = 0 :: non_neg_integer(),
    stats_per_channel = 0 :: non_neg_integer(),
    stats_per_queue = 0 :: non_neg_integer(),
    vhosts = 0 :: non_neg_integer(),
    node_stats = 0 :: non_neg_integer()
}).

-spec event_counts() -> [#event_counts{}].
event_counts() ->
    [
    % 1:
    % 1000 connection_created and connection_closed
        #event_counts{
            connections = 1000
        },
    % 2:
    % 1000000 connection_created and connection_closed
        #event_counts{
            connections = 1000000
        },
    % 3:
    % 1000 channel_created and channel_closed
        #event_counts{
            connections = 1, channels_per_connection = 1000
        },
    % 4:
    % 1000000 channel_created and channel_closed
        #event_counts{
            connections = 1, channels_per_connection = 1000000
        },
    % 5:
    % 1000 connection_created and connection_closed
    % 1000000 channel_created and channel_closed
        #event_counts{
            connections = 1000, channels_per_connection = 1000
        },
    % 6:
    % 1000 connection_stats
        #event_counts{
            connections = 1, channels_per_connection = 1, stats_per_connection = 1000
        },
    % 7:
    % 1000000 connection_stats
        #event_counts{
            connections = 1, channels_per_connection = 1, stats_per_connection = 1000000
        },
    % 8:
    % 1000 connection_created and connection_closed
    % 1000 channel_created and channel_closed
    % 1000000 connection_stats
        #event_counts{
            connections = 1000, channels_per_connection = 1, stats_per_connection = 1000
        },
    % 9:
    % 1000 connection_created and connection_closed
    % 1000000 channel_created and channel_closed
    % 1000000 connection_stats
        #event_counts{
            connections = 1000, channels_per_connection = 1000, stats_per_connection = 1000
        },
    % 10:
    % 1000 vhost_created and vhost_closed
        #event_counts{
            vhosts = 1000
        },
    % 11:
    % 1000000 vhost_created and vhost_closed
        #event_counts{
            vhosts = 1000000
        },
    % 12:
    % 100000 node_stats
        #event_counts{
            node_stats = 100000
        },
    % 13:
    % 1000 queue_created and queue_deleted
        #event_counts{
            connections = 1, channels_per_connection = 1, queues_per_channel = 1000
        },
    % 14:
    % 1000000 queue_created and queue_deleted
        #event_counts{
            connections = 1, channels_per_connection = 1, queues_per_channel = 1000000
          }, 
    % 15:
    % 1000 channel_stats
        #event_counts{
            connections = 1, channels_per_connection = 1, stats_per_channel = 1000,
            queues_per_channel = 1, stats_per_queue = 1
          },
    % 16:
    % 1000000 channel_stats
        #event_counts{
            connections = 1, channels_per_connection = 1, stats_per_channel = 1000000,
            queues_per_channel = 1, stats_per_queue = 1
          },
    % 17:
    % 1000 queue_stats
        #event_counts{
            connections = 1, channels_per_connection = 1, stats_per_channel = 1,
            queues_per_channel = 1, stats_per_queue = 1000
          },
    % 18:
    % 1000000 queue_stats
        #event_counts{
            connections = 1, channels_per_connection = 1, stats_per_channel = 1,
            queues_per_channel = 1, stats_per_queue = 1000000
          },
    % 19:
    % 1000 queue_created, queue_deleted
    % 1000 channel_stats
    % 100000 queue_stats
        #event_counts{
            connections = 1, channels_per_connection = 1, stats_per_channel = 1000,
            queues_per_channel = 1000, stats_per_queue = 100
        },

    % 20:
    % 1000 queue_created, queue_deleted
    % 1000 channel_stats
    % 10000000 queue_stats
        #event_counts{
            connections = 1, channels_per_connection = 1, stats_per_channel = 1000,
            queues_per_channel = 1000, stats_per_queue = 1000
        },

    % 21:
    % 1000 queue_created, queue_deleted
    % 100000 channel_stats
    % 100000 queue_stats
        #event_counts{
            connections = 1, channels_per_connection = 100, stats_per_channel = 1000,
            queues_per_channel = 10, stats_per_queue = 100
        },

    % 22:
    % 1000 queue_created, queue_deleted
    % 1000000 channel_stats
    % 100000000 queue_stats
        #event_counts{
            connections = 1, channels_per_connection = 100, stats_per_channel = 1000,
            queues_per_channel = 1000, stats_per_queue = 1000
        }
    ].

-spec generate_events({global, atom()}, pid(), string(), integer()) -> ok.
generate_events(Stats_DB, Mgmt_DB_Caller, CSV, Option) ->
    #event_counts{
        connections = N_Conns,
        channels_per_connection = N_Chans,
        queues_per_channel = N_Queues,
        stats_per_channel = N_Ch_Stats,
        stats_per_connection = N_Conn_Stats,
        stats_per_queue = N_Queue_Stats,
        vhosts = N_VHosts,
        node_stats = N_Nodes
    } = lists:nth(Option, event_counts()),
    Conns = [ pid_and_name(<<"Conn">>, I) || I <- lists:seq(1, N_Conns) ],
    Chans = [ pid_and_name(Conn, I)  || {_, Conn} <- Conns, I <- lists:seq(1, N_Chans) ],
    VHosts = [ name(<<"VHost">>, I) || I <- lists:seq(1, N_VHosts) ],
    Queues = [ name(<<"Queue">>, I) || I <- lists:seq(1, (N_Queues*N_Chans*N_Conns)) ],
    Exchanges = [ name(<<"Ex">>, I) || I <- lists:seq(1, (N_Queues*N_Chans*N_Conns)) ],
    QXs = [ queue_and_exchange(Q, X) || {Q, X} <- lists:zip(Queues, Exchanges) ],

    QProps = [ queue_props(Name) || Name <- Queues],
    XProps = [ exchange_props(Name) || Name <- Exchanges],
    QXProps= [ queue_exchange_props(QRes, XRes, random:uniform(50)) || {QRes, XRes} <- QXs],

    QStatProps = [ Props || _I <- lists:seq(1, N_Queue_Stats), Props <- QProps],
    QXStatProps = [ Props || _I <- lists:seq(1, N_Queue_Stats), Props <- QXProps],
    XStatProps = [ Props || _I <- lists:seq(1, N_Queue_Stats), Props <- XProps],

    ok = rabbit_mgmt_db_stress_stats:reset(),
    io:format("Generating ~p connection_created events.~n", [C1 = N_Conns]),
    Mgmt_DB_Caller ! {all_connections, N_Conns =< 1000},
    [ gen_server:cast(Stats_DB, connection_created(Pid, Name))
    || {Pid, Name} <- Conns
    ],
    Conns_Sample = [ Name || {_Pid, Name} <- Conns, random:uniform(N_Conns) < 100 ],
    Mgmt_DB_Caller ! {connections, Conns_Sample},
    io:format("Generating ~p channel_created events.~n", [C2 = N_Conns * N_Chans]),
    Mgmt_DB_Caller ! {all_channels, (N_Conns * N_Chans) =< 1000},
    [ gen_server:cast(Stats_DB, channel_created(Pid, Name))
    || {Pid, Name} <- Chans
    ],
    Chans_Sample = [ Name || {_Pid, Name} <- Chans, random:uniform(N_Conns * N_Chans) < 100 ],
    Mgmt_DB_Caller ! {channels, Chans_Sample},
    io:format("Generating ~p queue_created events.~n", [C3 = N_Conns * N_Chans * N_Queues]),
    [ gen_server:cast(Stats_DB, queue_created(Props))
    || Props <- QProps
    ],
    io:format("Generating ~p queue_stats events.~n", [C4 = N_Conns * N_Chans * N_Queues * N_Queue_Stats]),
    [ gen_server:cast(Stats_DB, queue_stats(Props))
    || Props <- QStatProps
    ],
    io:format("Generating ~p exchange_created events.~n", [C5 = N_Conns * N_Chans * N_Queues]),
    [ gen_server:cast(Stats_DB, exchange_created(Props))
    || Props <- XProps
    ],
    io:format("Generating ~p exchange_stats events.~n", [C6 = N_Conns * N_Chans * N_Queues * N_Queue_Stats]),
    [ gen_server:cast(Stats_DB, exchange_stats(Props))
    || Props <- XStatProps
    ],
    io:format("Generating ~p queue_exchange_stats events.~n", [C7 = N_Conns * N_Chans * N_Queues * N_Queue_Stats]),
    [ gen_server:cast(Stats_DB, queue_exchange_stats(Props))
    || Props <- QXStatProps
    ],
    io:format("Generating ~p channel_stats events.~n", [C8 = N_Conns * N_Chans * N_Ch_Stats]),
    [ gen_server:cast(Stats_DB, channel_stats(Pid, XStatProps, QXStatProps, QStatProps))
    || _I <- lists:seq(1, N_Ch_Stats), {Pid, _Name} <- Chans
    ],
    io:format("Generating ~p connection_stats events.~n", [C9 = N_Conn_Stats * N_Conns]),
    [ gen_server:cast(Stats_DB, connection_stats(Pid, random:uniform(50)))
    || _ <- lists:seq(1, N_Conn_Stats), {Pid, _Name} <- Conns
    ],
    io:format("Generating ~p queue_deleted events.~n", [C10 = N_Conns * N_Chans * N_Queues]),
    [ gen_server:cast(Stats_DB, queue_deleted(Props))
    || Props <- QProps
    ],
    io:format("Generating ~p exchange_deleted events.~n", [C10 = N_Conns * N_Chans * N_Queues]),
    [ gen_server:cast(Stats_DB, exchange_deleted(Props))
    || Props <- XProps
    ],
    io:format("Generating ~p channel_closed events.~n", [C11 = N_Conns * N_Chans]),
    Mgmt_DB_Caller ! {channels, []},
    [ gen_server:cast(Stats_DB, channel_closed(Pid))
    || {Pid, _Name} <- Chans
    ],
    io:format("Generating ~p connection_closed events.~n", [C12 = N_Conns]),
    Mgmt_DB_Caller ! {connections, []},
    [ gen_server:cast(Stats_DB, connection_closed(Pid))
    || {Pid, _Name} <- Conns
    ],
    io:format("Generating ~p vhost_created events.~n", [C13 = N_VHosts]),
    [ gen_server:cast(Stats_DB, vhost_created(Name))
    || Name <- VHosts
    ],
    VHosts_Sample = [ vhost(VHost) || VHost <- VHosts, random:uniform(N_VHosts) < 100 ],
    Mgmt_DB_Caller ! {vhosts, VHosts_Sample},
    timer:sleep(1000),
    Mgmt_DB_Caller ! {vhosts, []},
    io:format("Generating ~p vhost_deleted events.~n", [C14 = N_VHosts]),
    [ gen_server:cast(Stats_DB, vhost_deleted(Name))
    || Name <- VHosts
    ],
    io:format("Generating ~p node_stats events.~n", [C15 = N_Nodes]),
    [ gen_server:cast(Stats_DB, node_stats())
    || _ <- lists:seq(1, N_Nodes)
    ],
    N_Total_Casts = C1 + C2 + C3 + C4 + C5 + C6 + C7 + C8 +
                    C9 + C10 + C11 + C12 + C13 + C14 + C15,
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
    {spawn(fun () -> ok end), name(Prefix, I)}.

-spec name(binary(), non_neg_integer()) -> binary().
name(Prefix, I) when is_binary(Prefix), is_integer(I), I >= 0->
    Num = integer_to_binary(I),
    <<Prefix/binary, "_", Num/binary>>.

fine_stats_format(Props) ->
    Name = get_name_prop(Props),
    {Name, Props--[Name]}.

get_name_prop(Props) ->
    case proplists:lookup(name, Props) of
        {name, N} -> N;
        _         -> throw({"'name' undefined", Props})
    end.

% The timestamp is in milli seconds
-spec timestamp() -> non_neg_integer().
timestamp() ->
    {Mega, Secs, Micro} = os:timestamp(),
    1000000*1000*Mega + 1000*Secs + (Micro div 1000).

-spec now_to_micros({non_neg_integer(), non_neg_integer(), non_neg_integer()}) -> non_neg_integer().
now_to_micros({Mega, Sec, Micro}) ->
    1000000*1000000*Mega + 1000000*Sec + Micro.


-spec vhost_created(binary()) -> {event, #event{}}.
vhost_created(Name) when is_binary(Name) ->
    event(vhost_created, vhost(Name)).

-spec vhost_deleted(binary()) -> {event, #event{}}.
vhost_deleted(Name) when is_binary(Name) ->
    event(vhost_deleted, [{name, Name}]).

-spec connection_created(pid(), binary()) -> {event, #event{}}.
connection_created(Pid, Name) when is_pid(Pid), is_binary(Name) ->
    event(connection_created, [{pid, Pid}, {name, Name}]).

-spec connection_stats(pid(), non_neg_integer()) -> {event, #event{}}.
connection_stats(Pid, Oct)  when is_pid(Pid), is_integer(Oct), Oct >= 0->
    event(connection_stats, [{pid, Pid}, {recv_oct, Oct}]).

-spec connection_closed(pid()) -> {event, #event{}}.
connection_closed(Pid) when is_pid(Pid) ->
    event(connection_closed, [{pid, Pid}]).

%% ----------------------
%% Channel props & events
%% ----------------------
-spec channel_created(pid(), binary()) -> {event, #event{}}.
channel_created(Pid, Name) when is_pid(Pid), is_binary(Name) ->
    event(channel_created, [{pid, Pid}, {name, Name}]).

-spec channel_stats(pid(), list(), list(), list()) -> {event, #event{}}.
channel_stats(Pid, XStatsProps, QXStatsProps, QStatsProps) ->
    XStats1 = [ fine_stats_format(Props) || Props <- XStatsProps ],
    QXStats1 = [ fine_stats_format(Props) || Props <- QXStatsProps ],
    QStats1 = [ fine_stats_format(Props) || Props <- QStatsProps ],

    event(channel_stats, [
        {pid, Pid},
        {channel_exchange_stats, XStats1},
        {channel_queue_exchange_stats, QXStats1},
        {channel_queue_stats, QStats1}
    ]).

-spec channel_closed(pid()) -> {event, #event{}}.
channel_closed(Pid) when is_pid(Pid) ->
    event(channel_closed, [{pid, Pid}]).

%% --------------------
%% Queue props & events
%% --------------------
-spec queue_props(binary()) -> list().
queue_props(Name) when is_binary(Name) ->
    [{name, queue(Name)},
     {messages, random:uniform(50)},
     {messages_ready, random:uniform(50)},
     {messages_unacknowledged, random:uniform(50)},
     {durable, false},
     {auto_delete, false},
     {arguments, []},
     {owner_pid, ''},
     {exclusive, false},
     {deliver_no_ack, random:uniform(50)}].

-spec queue_created(list()) -> {event, #event{}}.
queue_created(QProps) when is_list(QProps) ->
    event(queue_created, QProps).

-spec queue_stats(list()) -> {event, #event{}}.
queue_stats(QProps) when is_list(QProps) ->
    event(queue_stats, QProps).

-spec queue_deleted(list()) -> {event, #event{}}.
queue_deleted(QStats) when is_list(QStats) ->
    event(queue_deleted, [get_name_prop(QStats)]).

%% -----------------------
%% Exchange props & events
%% -----------------------
-spec exchange_props(binary()) -> list().
exchange_props(Name) when is_binary(Name)  ->
    [{name, exchange(Name)}, {publish, random:uniform(50)}].

-spec exchange_created(list()) -> {event, #event{}}.
exchange_created(XProps) when is_list(XProps) ->
    event(exchange_created, [get_name_prop(XProps)]).

-spec exchange_stats(list()) -> {event, #event{}}.
exchange_stats(XProps) when is_list(XProps) ->
    event(exchange_stats, XProps).

-spec exchange_deleted(list()) -> {event, #event{}}.
exchange_deleted(XProps) when is_list(XProps) ->
    event(exchange_deleted, [get_name_prop(XProps)]).

%% -----------------------------
%% Queue exchange props & events
%% -----------------------------
-spec queue_exchange_props(#resource{}, #resource{}, non_neg_integer()) -> list().
queue_exchange_props(QRes=#resource{}, XRes=#resource{}, Publishes) when is_integer(Publishes) ->
    [{name, {QRes, XRes}}, {publish, Publishes}].

-spec queue_exchange_stats(list()) -> {event, #event{}}.
queue_exchange_stats(QXStats) ->
    event(queue_exchange_stats, QXStats).

%% -----------
%% Node events
%% -----------
-spec node_stats() -> {event, #event{}}.
node_stats() ->
    event(node_stats, [{name,rabbit@isis},
                       {partitions,[]},
                       {os_pid,<<"72705">>},
                       {fd_used,33},
                       {fd_total,16384},
                       {sockets_used,1},
                       {sockets_total,14653},
                       {mem_used,54491616},
                       {mem_limit,6393013862},
                       {mem_alarm,false},
                       {disk_free_limit,50000000},
                       {disk_free,145587232768},
                       {disk_free_alarm,false},
                       {proc_used,198},
                       {proc_total,1048576},
                       {rates_mode,basic},
                       {uptime,217354},
                       {run_queue,0},
                       {processors,4},
                       {exchange_types,[[{name,<<"topic">>},
                                         {description,<<"AMQP topic exchange, as per the AMQP specification">>},
                                         {enabled,true}],
                                        [{name,<<"fanout">>},
                                         {description,<<"AMQP fanout exchange, as per the AMQP specification">>},
                                         {enabled,true}],
                                        [{name,<<"direct">>},
                                         {description,<<"AMQP direct exchange, as per the AMQP specification">>},
                                         {enabled,true}],
                                        [{name,<<"headers">>},
                                         {description,<<"AMQP headers exchange, as per the AMQP specification">>},
                                         {enabled,true}]]},
                       {auth_mechanisms,[[{name,<<"AMQPLAIN">>},
                                          {description,<<"QPid AMQPLAIN mechanism">>},
                                          {enabled,true}],
                                         [{name,<<"PLAIN">>},
                                          {description,<<"SASL PLAIN authentication mechanism">>},
                                          {enabled,true}],
                                         [{name,<<"RABBIT-CR-DEMO">>},
                                          {description,<<"RabbitMQ Demo challenge-response authentication mechanism">>},
                                          {enabled,false}]]},
                       {applications,[[{name,amqp_client},
                                       {description,<<"RabbitMQ AMQP Client">>},
                                       {version,<<>>}],
                                      [{name,asn1},
                                       {description,<<"The Erlang ASN1 compiler version 3.0.3">>},
                                       {version,<<"3.0.3">>}],
                                      [{name,compiler},
                                       {description,<<"ERTS  CXC 138 10">>},
                                       {version,<<"5.0.3">>}],
                                      [{name,crypto},
                                       {description,<<"CRYPTO">>},
                                       {version,<<"3.4.2">>}],
                                      [{name,inets},
                                       {description,<<"INETS  CXC 138 49">>},
                                       {version,<<"5.10.4">>}],
                                      [{name,kernel},
                                       {description,<<"ERTS  CXC 138 10">>},
                                       {version,<<"3.1">>}],
                                      [{name,mnesia},
                                       {description,<<"MNESIA  CXC 138 12">>},
                                       {version,<<"4.12.4">>}],
                                      [{name,mochiweb},
                                       {description,<<"MochiMedia Web Server">>},
                                       {version,<<"2.13.0">>}],
                                      [{name,os_mon},
                                       {description,<<"CPO  CXC 138 46">>},
                                       {version,<<"2.3">>}],
                                      [{name,public_key},
                                       {description,<<"Public key infrastructure">>},
                                       {version,<<"0.22.1">>}],
                                      [{name,rabbit},
                                       {description,<<"RabbitMQ">>},
                                       {version,<<"0.0.0">>}],
                                      [{name,rabbit_common},{description,<<>>},{version,<<>>}],
                                      [{name,rabbitmq_management},
                                       {description,<<"RabbitMQ Management Console">>},
                                       {version,<<>>}],
                                      [{name,rabbitmq_management_agent},
                                       {description,<<"RabbitMQ Management Agent">>},
                                       {version,<<>>}],
                                      [{name,rabbitmq_web_dispatch},
                                       {description,<<"RabbitMQ Web Dispatcher">>},
                                       {version,<<>>}],
                                      [{name,sasl},
                                       {description,<<"SASL  CXC 138 11">>},
                                       {version,<<"2.4.1">>}],
                                      [{name,ssl},
                                       {description,<<"Erlang/OTP SSL application">>},
                                       {version,<<"5.3.8">>}],
                                      [{name,stdlib},
                                       {description,<<"ERTS  CXC 138 10">>},
                                       {version,<<"2.3">>}],
                                      [{name,syntax_tools},
                                       {description,<<"Syntax tools">>},
                                       {version,<<"1.6.17">>}],                [{name,webmachine},                 {description,<<"webmachine">>},
                                                                                {version,<<"git">>}],
                                      [{name,xmerl},
                                       {description,<<"XML parser">>},
                                       {version,<<"1.3.7">>}]]},
                       {contexts,[[{description,<<"RabbitMQ Management">>},
                                   {path,<<"/">>},
                                   {port,<<"15672">>}]]},
                       {log_file,<<"/var/folders/5q/vkjhc27d4w9_hc69p7q3vd1w0000gp/T/rabbitmq-test-instances/rabbit/log/rabbit.log">>},
                       {sasl_log_file,<<"/var/folders/5q/vkjhc27d4w9_hc69p7q3vd1w0000gp/T/rabbitmq-test-instances/rabbit/log/rabbit-sasl.log">>},
                       {db_dir,<<"/var/folders/5q/vkjhc27d4w9_hc69p7q3vd1w0000gp/T/rabbitmq-test-instances/rabbit/mnesia/rabbit">>},
                       {config_files,[<<"/etc/rabbitmq/rabbitmq.config (not found)">>]},
                       {net_ticktime,60},
                       {enabled_plugins,[amqp_client,mochiweb,rabbitmq_management,
                                         rabbitmq_management_agent,rabbitmq_web_dispatch,
                                         webmachine]},
                       {persister_stats, [{io_read_bytes,1},
                                          {io_read_count,1},
                                          {io_reopen_count,0},
                                          {io_seek_count,0},
                                          {io_sync_count,0},
                                          {io_write_bytes,0},
                                          {io_write_count,0},
                                          {mnesia_disk_tx_count,15},
                                          {mnesia_ram_tx_count,6},
                                          {msg_store_read_count,0},
                                          {msg_store_write_count,0},
                                          {queue_index_journal_write_count,0},
                                          {queue_index_read_count,0},
                                          {queue_index_write_count,0},
                                          {io_read_avg_time,0},
                                          {io_seek_avg_time,0},
                                          {io_sync_avg_time,0},
                                          {io_write_avg_time,0}]}]).

%#event{
%    type = consumer_created, % deps/rabbit/src/rabbit_amqqueue_process.erl:902
%    props = [
%        {consumer_tag, CTag},
%        {exclusive, Exclusive},
%        {ack_required, AckRequired},
%        {channel, ChPid},
%        {queue, QName},
%        {prefetch_count, PrefetchCount},
%        {arguments, Args}
%    ]
%}
%#event{
%    type = consumer_deleted, % deps/rabbit/src/rabbit_amqqueue_process.erl:913
%    props = [
%        {consumer_tag, ConsumerTag},
%        {channel, ChPid},
%        {queue, QName}
%    ]
%}

-spec event(atom(), list()) -> {event, #event{}}.
event(Type, Props) ->
    {event, #event{
        type = Type,
        props = Props,
        reference = none,
        timestamp = timestamp()
    }}.

-spec vhost(binary()) -> [{atom(), term()}].
vhost(Name) when is_binary(Name)->
    [{name, Name}, {tracing, false}].

-spec queue(binary()) -> #resource{}.
queue(Name) when is_binary(Name)->
    #resource{virtual_host = <<"/">>, kind = queue, name = Name}.

-spec exchange(binary()) -> #resource{}.
exchange(Name) when is_binary(Name)->
    #resource{virtual_host = <<"/">>, kind = exchange, name = Name}.

-spec queue_and_exchange(binary(), binary()) -> {#resource{}, #resource{}}.
queue_and_exchange(QName, XName) ->
    {queue(QName), exchange(XName)}.

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

