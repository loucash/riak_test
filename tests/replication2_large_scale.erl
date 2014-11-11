-module(replication2_large_scale).
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%% @doc The large scale test is to test:
%%
%% Properties
%%
%% large scale repl (fullsync + realtime)
%% real time balance working
%% fullsync not halted by nodes up/down/add/remove
%%realtime not halted by nodes up/down/add/remove
%%
-behavior(riak_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-export([confirm/0]).

-record(state, {a_up = [], a_down = [], b_up= [], b_down= []}).

-define(Conf,
        [{riak_kv, [{anti_entropy, {on, []}},
                    {anti_entropy_build_limit, {100, 1000}},
                    {anti_entropy_concurrency, 100}]},
         {riak_repl, [{realtime_connection_rebalance_max_delay_secs, 1},
                      {fullsync_on_connect, false}]}
        ]).

-define(SizeA, 5).
-define(SizeB, 5).

-define(Sleep, 120000).

confirm() ->
    {ANodes, BNodes} = repl_util:create_clusters_with_rt([{?SizeA, ?Conf}, {?SizeB,?Conf}], '<->'),
    State = #state{ a_up = ANodes, b_up = BNodes},
    repl_util:verify_correct_connection(ANodes),
    
    AllLoadGens = rt_config:get(perf_loadgens, ["localhost"]),
    case length(AllLoadGens) of
        1 ->
            start_basho_bench(ANodes ++ BNodes, AllLoadGens);
        N ->
            {ALoadGens, BLoadGens} = lists:split(N div 2, AllLoadGens),
            start_basho_bench(ANodes, ALoadGens),
            start_basho_bench(BNodes, BLoadGens)
    end,

    timer:sleep(?Sleep),
    State1 = node_a_down(State),
    State2 = node_a_down(State1),
    timer:sleep(?Sleep),
%%  repl_util:setup_rt(State1#state.a_up, '<-', State1#state.b_up),
    State3 = node_b_down(State2),
    timer:sleep(?Sleep),
    State4 = node_a_up(State3),
    timer:sleep(?Sleep),
    State5 = node_b_down(State4),
     timer:sleep(?Sleep),
    State6 = node_a_up(State5),
     timer:sleep(?Sleep),
    State7 = node_b_up(State6),

    run_full_sync(State7),
    rt_bench:stop_bench(),
    timer:sleep(?Sleep),
    run_full_sync(State7),
    pass.

run_full_sync(State) ->
    LeaderA = prepare_cluster(State#state.a_up, State#state.b_up),
    %% Perform fullsync of an empty cluster.
    rt:wait_until_aae_trees_built(State#state.a_up ++ State#state.b_up),
    {FullsyncTime, _} = timer:tc(repl_util,
                                  start_and_wait_until_fullsync_complete,
                                  [LeaderA]),
    lager:info("Fullsync time: ~p s", [FullsyncTime div 1000000]).

start_basho_bench(Nodes, LoadGens) ->
        PbIps = lists:map(fun(Node) ->
                              {ok, [{PB_IP, PB_Port}]} = rt:get_pb_conn_info(Node),
                              {PB_IP, PB_Port}
                      end, Nodes),

    LoadConfig = bacho_bench_config(PbIps),
    spawn(fun() -> rt_bench:bench(LoadConfig, Nodes, "50percentbackround", 1, false, LoadGens) end).

bacho_bench_config(HostList) ->
    BenchRate =
        rt_config:get(basho_bench_rate, 50),
    BenchDuration =
        rt_config:get(basho_bench_duration, infinity),
    KeyGen =
        rt_config:get(basho_bench_keygen, {int_to_bin_bigendian, {pareto_int, 10000000}}),
    ValGen =
        rt_config:get(basho_bench_valgen, {exponential_bin, 1000, 10000}),
    Operations =
        rt_config:get(basho_bench_operations, [{get, 5},{put, 1}]),
    Bucket =
        rt_config:get(basho_bench_bucket, <<"testbucket">>),
    Driver =
        rt_config:get(basho_bench_driver, riakc_pb), 
    ReportInterval =
         rt_config:get(basho_bench_report_interval, 5),
    
    rt_bench:config(BenchRate,
                    BenchDuration,
                    HostList, 
                    KeyGen,
                    ValGen,
                    Operations,
                    Bucket,
                    Driver,
                    ReportInterval).

node_a_down(State) ->
    node_a_down(State, lists:last(State#state.a_up)).

node_b_down(State) ->
    node_b_down(State, lists:last(State#state.b_up)).

node_a_up(State) ->
    node_a_up(State, lists:last(State#state.a_down)).

node_b_up(State) ->
    node_b_up(State, lists:last(State#state.b_down)).

node_a_down(State, Node) ->
    stop(Node),
    new_state(State, node_a_down, [Node]).
node_b_down(State, Node) ->
    stop(Node),
    new_state(State, node_b_down, [Node]).
node_a_up(State, Node) ->
    start(Node),
    new_state(State, node_a_up, [Node]).
node_b_up(State, Node) ->
    start(Node),
    new_state(State, node_b_up, [Node]).

new_state(S, node_a_down, Node) ->
    S#state{a_up = S#state.a_up -- Node,
            a_down = S#state.a_down ++ Node};

new_state(S, node_b_down, Node) ->
    S#state{b_up = S#state.b_up -- Node,
            b_down = S#state.b_down ++ Node};

new_state(S, node_a_up, Node) ->
    S#state{a_down = S#state.a_down -- Node,
            a_up = S#state.a_up ++ Node};

new_state(S, node_b_up, Node) ->
    S#state{b_down = S#state.b_down -- Node,
            b_up = S#state.b_up ++ Node}.

stop(Node) ->
    rt:stop(Node),
    rt:wait_until_unpingable(Node),
    timer:sleep(5000),
    true.
start(Node) ->
    rt:start(Node),
    rt:wait_until_ready(Node),
    timer:sleep(5000),
    true.



prepare_cluster([AFirst|_] = ANodes, [BFirst|_]) ->
    LeaderA = rpc:call(AFirst,
                       riak_core_cluster_mgr, get_leader, []),

    {ok, {IP, Port}} = rpc:call(BFirst,
                                application, get_env, [riak_core, cluster_mgr]),

    repl_util:connect_cluster(LeaderA, IP, Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    LeaderA.
