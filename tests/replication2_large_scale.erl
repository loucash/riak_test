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

-define(Conf,[{riak_kv, [{anti_entropy, {on, []}},
                         {anti_entropy_build_limit, {100, 1000}},
                         {anti_entropy_concurrency, 100}]},
              {riak_repl, [{realtime_connection_rebalance_max_delay_secs, 1},
                           {fullsync_on_connect, false}]}
             ]).

-define(SizeA, 3).
-define(SizeB, 3).

-define(BB_RATE,
        rt_config:get(basho_bench_rate, 10)).
-define(BB_DURATION,
        rt_config:get(basho_bench_duration, 2)).
-define(BB_KEYGEN,
        rt_config:get(basho_bench_keygen, {int_to_bin_bigendian, {pareto_int, 10000000}})).
-define(BB_VALGEN,
        rt_config:get(basho_bench_valgen, {exponential_bin, 1000, 10000})).
-define(BB_OPERATIONS,
        rt_config:get(basho_bench_operations, [{get, 5},{put, 1}])).
-define(BB_BUCKET,
        rt_config:get(basho_bench_bucket, <<"testbucket">>)).
-define(BB_DRIVER,
        rt_config:get(basho_bench_driver, riakc_pb)).


confirm() ->
    {ANodes, BNodes} = repl_util:create_clusters_with_rt([{?SizeA, ?Conf}, {?SizeB,?Conf}], '->'),
    State = #state{ a_up = ANodes, b_up = BNodes},
    %%verify_correct_connection(State),
    %%    rt_bench:bench(bacho_bench_config(), ["localhost"], "50percentbackround"),
    HostList = rt_config:get(rt_hostnames),

    LoadConfig = bacho_bench_config(HostList),
    spawn_link(fun() ->  rt_bench:bench(LoadConfig, HostList, "50percentbackround") end),

    timer:sleep(20000),

    LastA = lists:last(State#state.a_up),
    State1 = node_a_down(State, LastA),
    %%verify_correct_connection(State1),

    LastB = lists:last(State1#state.b_up),
    State2 = node_a_down(State1, LastB),


    %%verify_correct_connection(State2),

    repl_util:setup_rt(ANodes, '<-', BNodes),

    LastA = lists:last(State#state.a_up),
    State1 = node_a_down(State, LastA),
    %%verify_correct_connection(State1),

    LastB = lists:last(State1#state.b_up),
    State2 = node_a_down(State1, LastB),


    LeaderA = repl_aae_fullsync_util:prepare_cluster(ANodes, BNodes),
    %% Perform fullsync of an empty cluster.
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {FullsyncTime, _} = timer:tc(repl_util,
                                 start_and_wait_until_fullsync_complete,
                                 [LeaderA]),
    lager:info("Fullsync time: ~p", [FullsyncTime]),
    true.

verify_correct_connection(State) ->
    repl_util:verify_correct_connection(State#state.a_up, State#state.b_up).

bacho_bench_config(HostList) ->
    rt_bench:config(?BB_RATE, ?BB_DURATION, HostList, ?BB_KEYGEN, ?BB_VALGEN, ?BB_OPERATIONS).


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


