-module(riak_test_executor).

-behavior(gen_fsm).

%% API
-export([start_link/0,
         stop/0]).

%% gen_fsm callbacks
-export([init/1,
         %% wait_for_input/2,
         %% wait_for_input/3,
         %% request_nodes/2,
         %% request_nodes/3,
         %% run_test/2,
         %% run_test/3,
         %% wait_for_completion/2
         %% wait_for_completion/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the test executor
start_link() ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Stop the executor
-spec stop() -> ok | {error, term()}.
stop() ->
    gen_fsm:sync_send_all_state_event(?MODULE, stop, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Read the storage schedule and go to idle.

init(_Args) ->
    {ok, waiting_for_input, #state{}}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

%% @doc this fsm has no special upgrade process
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
