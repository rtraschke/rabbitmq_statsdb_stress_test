-module(rabbit_mgmt_db_stress_stats).

-behaviour(gen_server).

-export([start_link/0, stop/0, reset/0, get/1, message_queue_len/1, handle_cast_time/2]).

% Export the gen_server callback functions.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        code_change/3, terminate/2]).


-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:cast(?MODULE, stop).

-spec reset() -> ok.
reset() ->
    gen_server:call(?MODULE, reset).

-spec get(non_neg_integer()) -> term().
get(N) ->
    ok = wait_until_ready(N),
    gen_server:call(?MODULE, get, infinity).

-spec wait_until_ready(non_neg_integer()) -> ok.
wait_until_ready(N) ->
    case gen_server:call(?MODULE, {stats_are_ready, N}) of
        true -> ok;
        false -> timer:sleep(100), wait_until_ready(N)
    end.

-spec message_queue_len(non_neg_integer()) -> ok.
message_queue_len(Len) ->
    gen_server:cast(?MODULE, {message_queue_len, Len}).

-spec handle_cast_time(atom(), non_neg_integer()) -> ok.
handle_cast_time(Event, Time_Elapsed) ->
    gen_server:cast(?MODULE, {handle_cast_time, Event, Time_Elapsed}).


% We know that we are ready to give stats results when the expected
% number of handle_cast calls has been recorded. Because the monitored
% Rabbit MQ also call into the stats db, we'll add a fudge factor onto
% the expected number of casts.

-record(state, {
    msg_counts = [] :: [non_neg_integer()],
    handle_cast_times = [] :: [{atom(), [non_neg_integer()]}],
    handle_cast_count = 0 :: non_neg_integer()
}).

init([]) ->
    {ok, #state{}}.

handle_call(reset, _From, #state{}) ->
    {reply, ok, #state{}};
handle_call({stats_are_ready, N}, _From, #state{handle_cast_count = Cast_Count} = State) ->
    {reply, Cast_Count >= N, State};
handle_call(get, _From,
        #state{msg_counts=Msg_Counts, handle_cast_times = Cast_Times} = State) ->
    Msg_Stats = bear:get_statistics(lists:reverse(Msg_Counts)),
    Cast_Stats = [
        {Event, bear:get_statistics(lists:reverse(Event_Times))}
        || {Event, Event_Times} <- Cast_Times
    ],
    {reply, [{msg_counts, Msg_Stats} | Cast_Stats], State};
handle_call(_Request, _From, State) ->
    {reply, bad_request, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast({message_queue_len, Len}, #state{msg_counts=Msg_Counts} = State) ->
    {noreply, State#state{msg_counts = [Len | Msg_Counts]}};
handle_cast({handle_cast_time, Event, Micros},
        #state{handle_cast_times = Cast_Times, handle_cast_count = Cast_Count} = State) ->
    {noreply, State#state{
        handle_cast_times = add_event_cast_time(Event, Micros, Cast_Times),
        handle_cast_count = Cast_Count + 1
    }};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_Old, State, _Extra) ->
    {ok, State}.

-spec add_event_cast_time(atom(), non_neg_integer(), [{atom(), [non_neg_integer()]}])
        -> [{atom(), [non_neg_integer()]}].
add_event_cast_time(Event, Micros, Cast_Times) ->
    case lists:keyfind(Event, 1, Cast_Times) of
        false ->
            lists:keystore(Event, 1, Cast_Times, {Event, [Micros]});
        {Event, Event_Times} ->
            lists:keyreplace(Event, 1, Cast_Times, {Event, [Micros | Event_Times]})
    end.
