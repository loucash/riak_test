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
-export([start_link/0,
         stop/0]).

%% Need to export to use with `spawn_link'.
-export([return_to_exit/3]).
-export([run/4, metadata/0, metadata/1, function_name/2]).

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

-include("rt.hrl").
-include_lib("eunit/include/eunit.hrl").

%% -spec(metadata() -> [{atom(), term()}]).
%% @doc fetches test metadata from spawned test process
%% metadata() ->
%%     riak_test ! metadata,
%%     receive
%%         {metadata, TestMeta} -> TestMeta
%%     end.

%% metadata(Pid) ->
%%     riak_test ! {metadata, Pid},
%%     receive
%%         {metadata, TestMeta} -> TestMeta
%%     end.

-spec run(integer(), atom(), [{atom(), term()}], list()) -> [tuple()].
%% @doc Runs a module's run/0 function after setting up a log
%%      capturing backend for lager.  It then cleans up that backend
%%      and returns the logs as part of the return proplist.
run(TestModule, Outdir, TestMetaData, HarnessArgs) ->
    %% TODO: Need to make a lager backend that can separate out log
    %% messages to different files. Not sure what the effect of this
    %% will be in concurrent test execution scenarios.
    %% TODO: Check HarnessArgs for `UseRTLagerBackend' property
    add_lager_backend(TestModule, Outdir, false),
    BackendExtras = case proplists:get_value(multi_config, TestMetaData) of
                        undefined -> [];
                        Value -> [{multi_config, Value}]
                    end,
    Backend = rt_backend:set_backend(
                 proplists:get_value(backend, TestMetaData), BackendExtras),
    {PropsMod, PropsFun} = function_name(properties, TestModule, 0, rt_cluster),
    {SetupMod, SetupFun} = function_name(setup, TestModule, 2, rt_cluster),
    {ConfirmMod, ConfirmFun} = function_name(confirm, TestModule),
    {Status, Reason} =
        case check_prereqs(ConfirmMod) of
            true ->
                lager:notice("Running Test ~s", [TestModule]),
                execute(TestModule,
                        {PropsMod, PropsFun},
                        {SetupMod, SetupFun},
                        {ConfirmMod, ConfirmFun},
                        TestMetaData);
            not_present ->
                {fail, test_does_not_exist};
            _ ->
                {fail, all_prereqs_not_present}
        end,

    lager:notice("~s Test Run Complete", [TestModule]),
    {ok, Logs} = remove_lager_backend(),
    Log = unicode:characters_to_binary(Logs),

    RetList = [{test, TestModule}, {status, Status}, {log, Log}, {backend, Backend} | proplists:delete(backend, TestMetaData)],
    case Status of
        fail -> RetList ++ [{reason, iolist_to_binary(io_lib:format("~p", [Reason]))}];
        _ -> RetList
    end.

add_lager_backend(TestModule, Outdir, true) ->
    add_lager_backend(TestModule, Outdir, false),
    gen_event:add_handler(lager_event, riak_test_lager_backend, [rt_config:get(lager_level, info), false]),
    lager:set_loglevel(riak_test_lager_backend, rt_config:get(lager_level, info));
add_lager_backend(TestModule, Outdir, false) ->
    case Outdir of
        undefined -> ok;
        _ ->
            gen_event:add_handler(lager_event, lager_file_backend,
                {Outdir ++ "/" ++ atom_to_list(TestModule) ++ ".dat_test_output",
                 rt_config:get(lager_level, info), 10485760, "$D0", 1}),
            lager:set_loglevel(lager_file_backend, rt_config:get(lager_level, info))
    end.

remove_lager_backend() ->
    gen_event:delete_handler(lager_event, lager_file_backend, []),
    gen_event:delete_handler(lager_event, riak_test_lager_backend, []).

%% does some group_leader swapping, in the style of EUnit.
execute(TestModule, PropsModFun, SetupModFun, ConfirmModFun, TestMetaData) ->
    process_flag(trap_exit, true),
    OldGroupLeader = group_leader(),
    NewGroupLeader = riak_test_group_leader:new_group_leader(self()),
    group_leader(NewGroupLeader, self()),

    {0, UName} = rt:cmd("uname -a"),
    lager:info("Test Runner `uname -a` : ~s", [UName]),

    %% Pid = spawn_link(?MODULE, return_to_exit, [Mod, Fun, []]),
    Pid = spawn_link(test_fun(PropsModFun, SetupModFun, ConfirmModFun, TestMetaData)),
    Ref = case rt_config:get(test_timeout, undefined) of
        Timeout when is_integer(Timeout) ->
            erlang:send_after(Timeout, self(), test_took_too_long);
        _ ->
            undefined
    end,

    {Status, Reason} = rec_loop(Pid, TestModule, TestMetaData),
    case Ref of
        undefined ->
            ok;
        _ ->
            erlang:cancel_timer(Ref)
    end,
    riak_test_group_leader:tidy_up(OldGroupLeader),
    case Status of
        fail ->
            ErrorHeader = "================ " ++ atom_to_list(TestModule) ++ " failure stack trace =====================",
            ErrorFooter = [ $= || _X <- lists:seq(1,length(ErrorHeader))],
            Error = io_lib:format("~n~s~n~p~n~s~n", [ErrorHeader, Reason, ErrorFooter]),
            lager:error(Error);
        _ -> meh
    end,
    {Status, Reason}.

-spec test_fun({atom(), atom()}, {atom(), atom()}, {atom(), atom()}, proplists:proplist()) -> function().
test_fun({PropsMod, PropsFun}, {SetupMod, SetupFun}, ConfirmModFun, MetaData) ->
    fun() ->
            Properties = PropsMod:PropsFun(),
            case SetupMod:SetupFun(Properties, MetaData) of
                {ok, SetupData} ->
                    RollingUpgradeTest = rt_properties:get(rolling_upgrade, SetupData),
                    ConfirmFun = compose_confirm_fun(ConfirmModFun,
                                                     SetupData,
                                                     MetaData,
                                                     RollingUpgradeTest),

                    ConfirmFun();
                _ ->
                    fail
            end
    end.

compose_confirm_fun({ConfirmMod, ConfirmFun}, SetupData, MetaData, true) ->
    Nodes = rt_properties:get(nodes, SetupData),
    WaitForTransfers = rt_properties:get(wait_for_transfers, SetupData),
    UpgradeVersion = rt_properties:get(upgrade_version, SetupData),
    fun() ->
            InitialResult = ConfirmMod:ConfirmFun(SetupData, MetaData),
            OtherResults = [begin
                                ensure_all_nodes_running(Nodes),
                                _ = rt_node:upgrade(Node, UpgradeVersion),
                                _ = rt_cluster:maybe_wait_for_transfers(Nodes, WaitForTransfers),
                                ConfirmMod:ConfirmFun(SetupData, MetaData)
                            end || Node <- Nodes],
            lists:all(fun(R) -> R =:= pass end, [InitialResult | OtherResults])
    end;
compose_confirm_fun({ConfirmMod, ConfirmFun}, SetupData, MetaData, false) ->
    fun() ->
            ConfirmMod:ConfirmFun(SetupData, MetaData)
    end.

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

rec_loop(Pid, TestModule, TestMetaData) ->
    receive
        test_took_too_long ->
            exit(Pid, kill),
            {fail, test_timed_out};
        metadata ->
            Pid ! {metadata, TestMetaData},
            rec_loop(Pid, TestModule, TestMetaData);
        {metadata, P} ->
            P ! {metadata, TestMetaData},
            rec_loop(Pid, TestModule, TestMetaData);
        {'EXIT', Pid, normal} -> {pass, undefined};
        {'EXIT', Pid, Error} ->
            lager:warning("~s failed: ~p", [TestModule, Error]),
            {fail, Error}
    end.

%% A return of `fail' must be converted to a non normal exit since
%% status is determined by `rec_loop'.
%%
%% @see rec_loop/3
-spec return_to_exit(module(), atom(), list()) -> ok.
return_to_exit(Mod, Fun, Args) ->
    case apply(Mod, Fun, Args) of
        pass ->
            %% same as exit(normal)
            ok;
        fail ->
            exit(fail)
    end.

check_prereqs(Module) ->
    try Module:module_info(attributes) of
        Attrs ->
            Prereqs = proplists:get_all_values(prereq, Attrs),
            P2 = [ {Prereq, rt_local:which(Prereq)} || Prereq <- Prereqs],
            lager:info("~s prereqs: ~p", [Module, P2]),
            [ lager:warning("~s prereq '~s' not installed.", [Module, P]) || {P, false} <- P2],
            lists:all(fun({_, Present}) -> Present end, P2)
    catch
        _DontCare:_Really ->
            not_present
    end.
