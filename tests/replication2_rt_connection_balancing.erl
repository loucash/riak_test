%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
%%

-module(replication2_rt_connection_balancing).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(HB_TIMEOUT,  2000).

confirm() ->
    TestSetup = establish_balanced_cluster(),
    ?assertEqual(pass, verify_peer_connections(TestSetup)),
    teardown(TestSetup),
    pass.

establish_balanced_cluster() ->
    Conf = [{riak_repl, []}],

    rt:set_advanced_conf(all, Conf),

    [ANodes, BNodes] = rt:build_clusters([3, 3]),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    lager:info("Waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    lager:info("Naming A"),
    repl_util:name_cluster(AFirst, "A"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes)),

    lager:info("Naming B"),
    repl_util:name_cluster(BFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(BNodes)),

    lager:info("Connecting A to B"),
    connect_clusters(AFirst, BFirst),

    lager:info("Enabling realtime replication from A to B."),
    repl_util:enable_realtime(AFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes)),
    repl_util:start_realtime(AFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes)),

    {ANodes, BNodes}.

verify_peer_connections(TestSetup) ->
  lager:info("Verify we have chosen the right peer."),
  {[ANode1, ANode2, ANode3], _BNodes} = TestSetup,

  %% For this particular setup, nodes should always get exactly these peers:
  ?assertEqual("127.0.0.1:10066", get_rt_peer(ANode1)),
  ?assertEqual("127.0.0.1:10056", get_rt_peer(ANode2)),
  ?assertEqual("127.0.0.1:10046", get_rt_peer(ANode3)),
  pass.

teardown(TestSetup) ->
  {ANodes, BNodes} = TestSetup,
  rt:clean_cluster(ANodes),
  rt:clean_cluster(BNodes).

get_rt_peer(Node) ->
    Connections = rpc:call(Node, riak_repl2_rtsource_conn_sup, enabled, []),
    lager:info("Cons ~p", [Connections]),
    [{_Remote, Pid}]  = Connections,
    Status = rpc:call(Node, riak_repl2_rtsource_conn, status, [Pid]),
    Socket = proplists:get_value(socket, Status),
    lager:info("Socket: ~p", [Socket]),
    proplists:get_value(peername, Socket).


%% @doc Connect two clusters for replication using their respective
%%      leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port).
