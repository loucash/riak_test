-module(node_manager).

-behavior(gen_server).

%% API
-export([start_link/4,
         reserve_nodes/3,
         return_nodes/1,
         status/0,
         stop/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {nodes :: [string()],
                nodes_available :: [string()],
                version_map :: [{string(), [string()]}]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Nodes, VersionMap, DefaultVersion, UpgradePath) ->
    gen_server:start_link({local, ?MODULE}, [Nodes, VersionMap, DefaultVersion, UpgradePath], []).

-spec reserve_nodes(pos_integer(), [string()], function()) -> ok.
reserve_nodes(NodeCount, Versions, NotifyFun) ->
    gen_server:cast(?MODULE, {reserve_nodes, NodeCount, Versions, NotifyFun}).

-spec return_nodes([string()]) -> ok.
return_nodes(Nodes) ->
    gen_server:cast(?MODULE, {return_nodes, Nodes}).

-spec status() -> [{atom(), list()}].
status() ->
    gen_server:call(?MODULE, status, infinity).

-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% TODO Need to receive `upgrade_path' as well
init([Nodes, VersionMap, DefaultVersion, undefined]) ->
    SortedNodes = lists:sort(Nodes),
    %% Deploy nodes with `DefaultVersion'
    deploy_nodes()
    {ok, #state{nodes=SortedNodes,
                nodes_available=SortedNodes,
                version_map=VersionMap}};
init([Nodes, VersionMap, _, [InitialVersion | _]]) ->
    SortedNodes = lists:sort(Nodes),
    %% Deploy nodes using the first entry from `UpgradeList'

    {ok, #state{nodes=SortedNodes,
                nodes_available=SortedNodes,
                version_map=VersionMap}}.

handle_call(status, _From, State) ->
    Status = [{nodes, State#state.nodes},
              {nodes_available, State#state.nodes_available},
              {version_map, State#state.version_map}],
    {reply, Status, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({reserve_nodes, Count, Versions, NotifyFun}, State) ->
    {Result, NodesNowAvailable} =
        reserve(Count, Versions, State#state.nodes_available),
    NotifyFun(Result),
    {noreply, State#state{nodes_available=NodesNowAvailable}};
handle_cast({return_nodes, Nodes}, State) ->
    NodesAvailable = State#state.nodes_available,
    NodesNowAvailable = lists:merge(lists:sort(Nodes), NodesAvailable),
    {noreply, State#state{nodes_available=NodesNowAvailable}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% TODO: Circle back to add version checking after get
%% basic test execution functioning
reserve(Count, _Versions, NodesAvailable)
  when Count > length(NodesAvailable) ->
    {not_enough_nodes, NodesAvailable};
reserve(Count, _Versions, NodesAvailable)
  when Count =:= length(NodesAvailable) ->
    {NodesAvailable, []};
reserve(Count, _Versions, NodesAvailable) ->
    lists:split(Count, NodesAvailable).

-spec deploy_nodes(list(test_node())) -> [string()].
deploy_nodes(Nodes, Version) ->
    %% TODO Use `root_path' + `Version' to launch nodes in rtdev specifically
    %% TODO Need to use harness-specific functions to do this

    NodeConfig = [rt_config:version_to_config(Version) ||
                     Version <- Versions],
    %% Nodes = rt_harness:deploy_nodes(NodeConfig),

    Path = relpath(root),
    lager:info("Riak path: ~p", [Path]),
    %% TODO: NumNodes, NodesN, and Nodes should come from the
    %% harnesses involved
    %% rt_harness:nodes(length(NodeConfig)),
    %% NodeMap = orddict:from_list(lists:zip(Nodes, NodesN)),
    %% {Versions, Configs} = lists:unzip(NodeConfig),
    %% VersionMap = lists:zip(NodesN, Versions),

    %% %% Check that you have the right versions available
    %% [check_node(Version) || Version <- VersionMap],
    %% rt_config:set(rt_nodes, NodeMap),
    %% rt_config:set(rt_versions, VersionMap),

    %% create_dirs(Nodes),
    %% Perform harness-specific configuration
    rt_harness:configure_nodes(Nodes, Configs),

    %% Start nodes
    rt:pmap(fun rt_node:start/1, Nodes),

    %% Ensure nodes started
    [ok = rt:wait_until_pingable(N) || N <- Nodes],

    %% %% Enable debug logging
    %% [rpc:call(N, lager, set_loglevel, [lager_console_backend, debug]) || N <- Nodes],

    %% We have to make sure that riak_core_ring_manager is running before we can go on.
    [ok = rt:wait_until_registered(N, riak_core_ring_manager) || N <- Nodes],

    %% Ensure nodes are singleton clusters
    [ok = rt_ring:check_singleton_node(?DEV(N)) || {N, Version} <- VersionMap,
                                              Version /= "0.14.2"],

    lager:info("Deployed nodes: ~p", [Nodes]),

    %% Wait for services to start
    lager:info("Waiting for services ~p to start on ~p.", [Services, Nodes]),
    [ ok = rt:wait_for_service(Node, Service) || Node <- Nodes,
                                              Service <- Services ],
    Nodes.
