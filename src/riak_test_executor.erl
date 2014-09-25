-module(riak_test_executor).

-behavior(gen_fsm).

%% API
-export([start_link/1,
         stop/0]).

%% gen_fsm callbacks
-export([init/1,
         gather_properties/2,
         gather_properties/3,
         request_nodes/2,
         request_nodes/3,
         launch_test/2,
         launch_test/3,
         wait_for_completion/2,
         wait_for_completion/3,
         report_results/2,
         report_results/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {tests_remaining :: [atom()],
                tests_running :: [atom()]}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the test executor
-spec start_link([{atom(), term()}]) -> {ok, pid()} | ignore | {error, term()}.
start_link(Tests) ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, Tests, []).

%% @doc Stop the executor
-spec stop() -> ok | {error, term()}.
stop() ->
    gen_fsm:sync_send_all_state_event(?MODULE, stop, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init(_Args) ->
    {ok, gather_properties, #state{}}.

%% @doc there are no all-state events for this fsm
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Handle synchronous events that should be handled
%% the same regardless of the current state.
-spec handle_sync_event(term(), term(), atom(), #state{}) ->
                               {reply, term(), atom(), #state{}}.
handle_sync_event(_Event, _From, _StateName, _State) ->
    {reply, ok, ok, _State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

%% @doc this fsm has no special upgrade process
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.


%%% Asynchronous call handling functions for each FSM state

gather_properties(_Event, _State) ->
    ok.

request_nodes(_Event, _State) ->
    ok.

launch_test(_Event, _State) ->
    ok.

wait_for_completion(_Event, _State) ->
    ok.

report_results(_Event, _State) ->
    ok.

%% Synchronous call handling functions for each FSM state

gather_properties(_Event, _From, _State) ->
    ok.

request_nodes(_Event, _From, _State) ->
    ok.

launch_test(_Event, _From, _State) ->
    ok.

wait_for_completion(_Event, _From, _State) ->
    ok.

report_results(_Event, _From, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
