%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc riak_test_runner runs a riak_test module's run/0 function.
-module(riak_test_runner).

-behavior(gen_fsm).

%% API
-export([start/3,
         send_event/2,
         stop/0]).

-export([function_name/2]).

%% gen_fsm callbacks
-export([init/1,
         setup/2,
         setup/3,
         execute/2,
         execute/3,
         wait_for_completion/2,
         wait_for_completion/3,
         wait_for_upgrade/2,
         wait_for_upgrade/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("rt.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {test_module :: atom(),
                properties :: proplists:proplist(),
                metadata :: term(),
                backend :: atom(),
                test_timeout :: integer(),
                execution_pid :: pid(),
                group_leader :: pid(),
                start_time :: erlang:timestamp(),
                setup_modfun :: {atom(), atom()},
                confirm_modfun :: {atom(), atom()},
                backend_check :: atom(),
                prereq_check :: atom(),
                current_version :: string(),
                remaining_versions :: [string()],
                test_results :: [term()]}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the test executor
start(TestModule, Backend, Properties) ->
    Args = [TestModule, Backend, Properties],
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, Args, []).

send_event(Pid, Msg) ->
    gen_fsm:send_event(Pid, Msg).

%% @doc Stop the executor
-spec stop() -> ok | {error, term()}.
stop() ->
    gen_fsm:sync_send_all_state_event(?MODULE, stop, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Read the storage schedule and go to idle.
%% compose_test_datum(Version, Project, undefined, undefined) ->
init([TestModule, Backend, Properties]) ->
    lager:debug("Started riak_test_runnner with pid ~p", [self()]),
    Project = list_to_binary(rt_config:get(rt_project, "undefined")),
    MetaData = [{id, -1},
                {platform, <<"local">>},
                {version, rt:get_version()},
                {project, Project}],
    TestTimeout = rt_config:get(test_timeout, rt_config:get(rt_max_wait_time)),
    SetupModFun = function_name(setup, TestModule, 2, rt_cluster),
    {ConfirmMod, _} = ConfirmModFun = function_name(confirm, TestModule),
    BackendCheck = check_backend(Backend,
                                 rt_properties:get(valid_backends, Properties)),
    PreReqCheck = check_prereqs(ConfirmMod),
    State = #state{test_module=TestModule,
                   properties=Properties,
                   metadata=MetaData,
                   backend=Backend,
                   test_timeout=TestTimeout,
                   setup_modfun=SetupModFun,
                   confirm_modfun=ConfirmModFun,
                   backend_check=BackendCheck,
                   prereq_check=PreReqCheck,
                   group_leader=group_leader()},
    {ok, setup, State, 0}.

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

%% Asynchronous call handling functions for each FSM state

setup(timeout, State=#state{backend_check=false}) ->
    notify_executor({skipped, invalid_backend}, State),
    cleanup(State),
    {stop, normal, State};
setup(timeout, State=#state{prereq_check=false}) ->
    notify_executor({fail, prereq_check_failed}, State),
    cleanup(State),
    {stop, normal, State};
setup(timeout, State=#state{backend=Backend,
                            properties=Properties}) ->
    NewGroupLeader = riak_test_group_leader:new_group_leader(self()),
    group_leader(NewGroupLeader, self()),

    {0, UName} = rt:cmd("uname -a"),
    lager:info("Test Runner `uname -a` : ~s", [UName]),

    Nodes = rt_properties:get(nodes, Properties),
    {StartVersion, OtherVersions} = test_versions(Properties),
    Config = rt_backend:set(Backend, rt_properties:get(config, Properties)),
    node_manager:deploy_nodes(Nodes,
                              StartVersion,
                              Config,
                              notify_fun()),
    UpdState = State#state{current_version=StartVersion,
                           remaining_versions=OtherVersions},
    {next_state, execute, UpdState};
setup(_Event, _State) ->
    ok.

execute({nodes_deployed, _}, State) ->
    #state{test_module=TestModule,
           properties=Properties,
           setup_modfun={SetupMod, SetupFun},
           confirm_modfun=ConfirmModFun,
           metadata=MetaData,
           test_timeout=TestTimeout} = State,
    lager:notice("Running Test ~s", [TestModule]),

    StartTime = os:timestamp(),
    %% Perform test setup which includes clustering of the nodes if
    %% required by the test properties. The cluster information is placed
    %% into the properties record and returned by the `setup' function.
    UpdState =
        case SetupMod:SetupFun(Properties, MetaData) of
            {ok, UpdProperties} ->
                Pid = spawn_link(test_fun(UpdProperties,
                                          ConfirmModFun,
                                          MetaData)),
                State#state{execution_pid=Pid,
                            properties=UpdProperties,
                            start_time=StartTime};
            _ ->
                ?MODULE:send_event(self(), test_result({fail, test_setup_failed})),
                State#state{start_time=StartTime}
        end,
    {next_state, wait_for_completion, UpdState, TestTimeout};
execute(_Event, _State) ->
    {next_state, execute, _State}.

%% Simple function to hide the details of the message wrapping
test_result(Result) ->
    {test_result, Result}.

wait_for_completion(timeout, State) ->
    %% Test timed out
    notify_executor(timeout, State),
    cleanup(State),
    {stop, normal, State};
wait_for_completion({test_result, Result}, State=#state{remaining_versions=[]}) ->
    %% TODO: Format results for aggregate test runs if needed. For
    %% upgrade tests with failure return which versions had failure
    %% along with reasons.
    %% TODO: Calculate test duration and include with results reported
    %% to executor
    _EndTime = os:timestamp(),
    notify_executor(Result, State),
    cleanup(State),
    {stop, normal, State};
wait_for_completion({test_result, Result}, State) ->
    #state{backend=Backend,
           test_results=TestResults,
           current_version=CurrentVersion,
           remaining_versions=[NextVersion | RestVersions],
           properties=Properties} = State,
    Config = rt_backend:set(Backend, rt_properties:get(config, Properties)),
    Nodes = rt_properties:get(nodes, Properties),
    %% TODO: Verify this is needed
    ensure_all_nodes_running(Nodes),
    node_manager:upgrade_nodes(Nodes, CurrentVersion, NextVersion, Config, notify_fun()),
    UpdState = State#state{test_results=[Result | TestResults],
                           current_version=NextVersion,
                           remaining_versions=RestVersions},
    {next_state, upgrade, UpdState};
wait_for_completion(_Msg, _State) ->
    {next_state, wait_for_completion, _State}.

wait_for_upgrade(nodes_upgraded, State) ->
    #state{properties=Properties,
           confirm_modfun=ConfirmModFun,
           metadata=MetaData,
           test_timeout=TestTimeout} = State,

    %% TODO: Maybe wait for transfers. Probably should be
    %% a call to an exported function in `rt_cluster'
    Pid = spawn_link(test_fun(Properties,
                              ConfirmModFun,
                              MetaData)),
    UpdState = State#state{execution_pid=Pid},
    {next_state, wait_for_completion, UpdState, TestTimeout};
wait_for_upgrade(_Event, _State) ->
    {next_state, wait_for_upgrade, _State}.

%% Synchronous call handling functions for each FSM state

setup(_Event, _From, _State) ->
    ok.

execute(_Event, _From, _State) ->
    ok.

wait_for_completion(_Event, _From, _State) ->
    ok.

wait_for_upgrade(_Event, _From, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec test_fun(rt_properties:properties(), {atom(), atom()}, proplists:proplist()) ->
                      function().
test_fun(Properties, {ConfirmMod, ConfirmFun}, MetaData) ->
    fun() ->
            TestResult = ConfirmMod:ConfirmFun(Properties, MetaData),
            ?MODULE:send_event(self(), test_result(TestResult))
    end.

%% compose_confirm_fun({ConfirmMod, ConfirmFun}, SetupData, MetaData, true) ->
%%     Nodes = rt_properties:get(nodes, SetupData),
%%     WaitForTransfers = rt_properties:get(wait_for_transfers, SetupData),
%%     UpgradeVersion = rt_properties:get(upgrade_version, SetupData),
%%     fun() ->
%%             InitialResult = ConfirmMod:ConfirmFun(SetupData, MetaData),
%%             OtherResults = [begin
%%                                 ensure_all_nodes_running(Nodes),
%%                                 _ = rt_node:upgrade(Node, UpgradeVersion),
%%                                 _ = rt_cluster:maybe_wait_for_transfers(Nodes, WaitForTransfers),
%%                                 ConfirmMod:ConfirmFun(SetupData, MetaData)
%%                             end || Node <- Nodes],
%%             lists:all(fun(R) -> R =:= pass end, [InitialResult | OtherResults])
%%     end;
%% compose_confirm_fun({ConfirmMod, ConfirmFun}, SetupData, MetaData, false) ->
%%     fun() ->
%%             ConfirmMod:ConfirmFun(SetupData, MetaData)
%%     end.

ensure_all_nodes_running(Nodes) ->
    [begin
         ok = rt_node:start_and_wait(Node),
         ok = rt:wait_until_registered(Node, riak_core_ring_manager)
     end || Node <- Nodes].

function_name(confirm, TestModule) ->
    TMString = atom_to_list(TestModule),
    Tokz = string:tokens(TMString, ":"),
    case length(Tokz) of
        1 -> {TestModule, confirm};
        2 ->
            [Module, Function] = Tokz,
            {list_to_atom(Module), list_to_atom(Function)}
    end.

function_name(FunName, TestModule, Arity, Default) when is_atom(TestModule) ->
    case erlang:function_exported(TestModule, FunName, Arity) of
        true ->
            {TestModule, FunName};
        false ->
            {Default, FunName}
    end.

%% -spec run(integer(), atom(), [{atom(), term()}], list()) -> [tuple()].
%% %% @doc Runs a module's run/0 function after setting up a log
%% %%      capturing backend for lager.  It then cleans up that backend
%% %%      and returns the logs as part of the return proplist.
%% run(TestModule, Outdir, TestMetaData, HarnessArgs) ->
%%     %% TODO: Need to make a lager backend that can separate out log
%%     %% messages to different files. Not sure what the effect of this
%%     %% will be in concurrent test execution scenarios.
%%     %% TODO: Check HarnessArgs for `UseRTLagerBackend' property
%%     add_lager_backend(TestModule, Outdir, false),
%%     %% BackendExtras = case proplists:get_value(multi_config, TestMetaData) of
%%     %%                     undefined -> [];
%%     %%                     Value -> [{multi_config, Value}]
%%     %%                 end,
%%     %% Backend = rt_backend:set_backend(
%%     %%              proplists:get_value(backend, TestMetaData), BackendExtras),
%%     %% {PropsMod, PropsFun} = function_name(properties, TestModule, 0, rt_cluster),
%%     {SetupMod, SetupFun} = function_name(setup, TestModule, 2, rt_cluster),
%%     {ConfirmMod, ConfirmFun} = function_name(confirm, TestModule),
%%     {Status, Reason} =
%%         case check_prereqs(ConfirmMod) of
%%             true ->
%%                 lager:notice("Running Test ~s", [TestModule]),
%%                 execute(TestModule,
%%                         {PropsMod, PropsFun},
%%                         {SetupMod, SetupFun},
%%                         {ConfirmMod, ConfirmFun},
%%                         TestMetaData);
%%             not_present ->
%%                 {fail, test_does_not_exist};
%%             _ ->
%%                 {fail, all_prereqs_not_present}
%%         end,

    %% lager:notice("~s Test Run Complete", [TestModule]),
    %% {ok, Logs} = remove_lager_backend(),
    %% Log = unicode:characters_to_binary(Logs),

    %% RetList = [{test, TestModule}, {status, Status}, {log, Log}, {backend, Backend} | proplists:delete(backend, TestMetaData)],
    %% case Status of
    %%     fail -> RetList ++ [{reason, iolist_to_binary(io_lib:format("~p", [Reason]))}];
    %%     _ -> RetList
    %% end.


%% does some group_leader swapping, in the style of EUnit.
%% execute(TestModule, PropsModFun, SetupModFun, ConfirmModFun, TestMetaData) ->


    %% Pid = spawn_link(?MODULE, return_to_exit, [Mod, Fun, []]),
    %% Pid = spawn_link(test_fun(PropsModFun, SetupModFun, ConfirmModFun, TestMetaData)),
    %% Ref = case rt_config:get(test_timeout, undefined) of
    %%     Timeout when is_integer(Timeout) ->
    %%         erlang:send_after(Timeout, self(), test_took_too_long);
    %%     _ ->
    %%         undefined
    %% end,

    %% {Status, Reason} = rec_loop(Pid, TestModule, TestMetaData),

    %% riak_test_group_leader:tidy_up(OldGroupLeader),
    %% case Status of
    %%     fail ->
    %%         ErrorHeader = "================ " ++ atom_to_list(TestModule) ++ " failure stack trace =====================",
    %%         ErrorFooter = [ $= || _X <- lists:seq(1,length(ErrorHeader))],
    %%         Error = io_lib:format("~n~s~n~p~n~s~n", [ErrorHeader, Reason, ErrorFooter]),
    %%         lager:error(Error);
    %%     _ -> meh
    %% end,
    %% {Status, Reason}.

%% add_lager_backend(TestModule, Outdir, true) ->
%%     add_lager_backend(TestModule, Outdir, false),
%%     gen_event:add_handler(lager_event, riak_test_lager_backend, [rt_config:get(lager_level, info), false]),
%%     lager:set_loglevel(riak_test_lager_backend, rt_config:get(lager_level, info));
%% add_lager_backend(TestModule, Outdir, false) ->
%%     case Outdir of
%%         undefined -> ok;
%%         _ ->
%%             gen_event:add_handler(lager_event, lager_file_backend,
%%                 {Outdir ++ "/" ++ atom_to_list(TestModule) ++ ".dat_test_output",
%%                  rt_config:get(lager_level, info), 10485760, "$D0", 1}),
%%             lager:set_loglevel(lager_file_backend, rt_config:get(lager_level, info))
%%     end.

%% remove_lager_backend() ->
%%     gen_event:delete_handler(lager_event, lager_file_backend, []),
%%     gen_event:delete_handler(lager_event, riak_test_lager_backend, []).


%% A return of `fail' must be converted to a non normal exit since
%% status is determined by `rec_loop'.
%%
%% @see rec_loop/3
%% -spec return_to_exit(module(), atom(), list()) -> ok.
%% return_to_exit(Mod, Fun, Args) ->
%%     case apply(Mod, Fun, Args) of
%%         pass ->
%%             %% same as exit(normal)
%%             ok;
%%         fail ->
%%             exit(fail)
%%     end.

-spec check_backend(atom(), all | [atom()]) -> boolean().
check_backend(_Backend, all) ->
    true;
check_backend(Backend, ValidBackends) ->
    lists:member(Backend, ValidBackends).

%% Check the prequisites for executing the test
check_prereqs(Module) ->
    Attrs = Module:module_info(attributes),
    Prereqs = proplists:get_all_values(prereq, Attrs),
    P2 = [{Prereq, rt_local:which(Prereq)} || Prereq <- Prereqs],
    lager:info("~s prereqs: ~p", [Module, P2]),
    [lager:warning("~s prereq '~s' not installed.",
                   [Module, P]) || {P, false} <- P2],
    lists:all(fun({_, Present}) -> Present end, P2).

notify_fun() ->
    fun(X) ->
            ?MODULE:send_event(self(), X)
    end.


cleanup(#state{group_leader=OldGroupLeader}) ->
    riak_test_group_leader:tidy_up(OldGroupLeader).

notify_executor(timeout, #state{test_module=Test}) ->
    Notification = {test_complete, Test, self(), {fail, timeout}},
    riak_test_executor:send_event(Notification);
notify_executor(pass, #state{test_module=Test}) ->
    Notification = {test_complete, Test, self(), pass},
    riak_test_executor:send_event(Notification);
notify_executor(FailResult, #state{test_module=Test}) ->
    Notification = {test_complete, Test, self(), FailResult},
    riak_test_executor:send_event(Notification).

test_versions(Properties) ->
    StartVersion = rt_properties:get(start_version, Properties),
    UpgradePath = rt_properties:get(upgrade_path, Properties),
    case UpgradePath of
        undefined ->
            {StartVersion, []};
        [] ->
            {StartVersion, []};
        _ ->
            [UpgradeHead | Rest] = UpgradePath,
            {UpgradeHead, Rest}
    end.
