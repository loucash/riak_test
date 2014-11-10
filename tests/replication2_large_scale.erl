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

confirm() ->
    {ANodes, BNodes} = repl_util:create_clusters_with_rt([{?SizeA, ?Conf}, {?SizeB,?Conf}], '->'),
    State = #state{ a_up = ANodes, b_up = BNodes},
    repl_util:verify_correct_connection(ANodes),
    AllNodes = ANodes ++ BNodes,
    
    PbIps = lists:map(fun(Node) ->
                              {ok, [{PB_IP, PB_Port}]} = rt:get_pb_conn_info(Node),
                              {PB_IP, PB_Port}
                      end, AllNodes),

    LoadConfig = bacho_bench_config(PbIps),
    spawn(fun() -> rt_bench:bench(LoadConfig, AllNodes, "50percentbackround") end),

    timer:sleep(20000),

    LastA = lists:last(State#state.a_up),
    State1 = node_a_down(State, LastA),

    rt_bench:stop_bench(),

    State2 = node_a_down(State1, lists:last(State1#state.b_up)),
%%  repl_util:setup_rt(State1#state.a_up, '<-', State1#state.b_up),
    State3 = node_a_down(State2, lists:last(State2#state.a_up)),
    State4 = node_a_down(State3, lists:last(State3#state.b_up)),
    
    State5 = node_a_down(State4, lists:last(State4#state.b_down)),
    State6 = node_a_down(State5, lists:last(State5#state.a_down)),
    _State7 = node_a_down(State6, lists:last(State6#state.b_down)),



    LeaderA = repl_aae_fullsync_util:prepare_cluster(ANodes, BNodes),
    %% Perform fullsync of an empty cluster.
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {FullsyncTime, _} = timer:tc(repl_util,
                                 start_and_wait_until_fullsync_complete,
                                 [LeaderA]),
    lager:info("Fullsync time: ~p", [FullsyncTime]),

    pass.


bacho_bench_config(HostList) ->
    BenchRate =
        rt_config:get(basho_bench_rate, 30),
    BenchDuration =
        rt_config:get(basho_bench_duration, 20),
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
    
    rt_bench:config(BenchRate,
                    BenchDuration,
                    HostList, 
                    KeyGen,
                    ValGen,
                    Operations,
                    Bucket,
                    Driver).


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
    timer:sleep(15000),
    true.
start(Node) ->
    rt:start(Node),
    rt:wait_until_ready(Node),
    timer:sleep(15000),
    true.


