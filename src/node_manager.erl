-module(node_manager).

-behavior(gen_server).

%% API
-export([start_link/4,
         reserve_nodes/4,
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
                nodes_deployed=[] :: [string()],
                initial_version :: string(),
                version_map :: [{string(), [string()]}]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Nodes, VersionMap, DefaultVersion, UpgradePath) ->
    Args = [Nodes, VersionMap, DefaultVersion, UpgradePath],
    gen_server:start_link({local, ?MODULE}, Args, []).

-spec reserve_nodes(pos_integer(), [string()], term(), function()) -> ok.
reserve_nodes(NodeCount, Versions, Config, NotifyFun) ->
    gen_server:cast(?MODULE, {reserve_nodes, NodeCount, Versions, Config, NotifyFun}).

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

%% Initial node deployment is deferred until the first call to
init([Nodes, VersionMap, DefaultVersion, undefined]) ->
    SortedNodes = lists:sort(Nodes),
    {ok, #state{nodes=SortedNodes,
                nodes_available=SortedNodes,
                initial_version=DefaultVersion,
                version_map=VersionMap}};
init([Nodes, VersionMap, _, [InitialVersion | _]]) ->
    SortedNodes = lists:sort(Nodes),
    {ok, #state{nodes=SortedNodes,
                nodes_available=SortedNodes,
                initial_version=InitialVersion,
                version_map=VersionMap}}.

handle_call(status, _From, State) ->
    Status = [{nodes, State#state.nodes},
              {nodes_available, State#state.nodes_available},
              {version_map, State#state.version_map}],
    {reply, Status, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({reserve_nodes, Count, Versions, Config, NotifyFun}, State) ->
    {Result, UpdState} =
        reserve(Count, Versions, Config, State),
    NotifyFun(Result),
    {noreply, UpdState};
handle_cast({return_nodes, Nodes}, State) ->
    %% TODO: Stop nodes, clean data dirs, and restart
    %% so they are ready for next use.
    NodesAvailable = State#state.nodes_available,
    NodesNowAvailable = lists:merge(lists:sort(Nodes), NodesAvailable),
    {noreply, State#state{nodes_available=NodesNowAvailable}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _) ->
    %% TODO: Stop and reset all deployed nodes
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% TODO: Circle back to add version checking after get
%% basic test execution functioning
reserve(Count, _Versions, _Config, State=#state{nodes_available=NodesAvailable})
  when Count > length(NodesAvailable) ->
    {not_enough_nodes, State};
reserve(Count, _Versions, Config, State=#state{nodes_available=NodesAvailable,
                                               nodes_deployed=NodesDeployed,
                                               initial_version=InitialVersion})
  when Count =:= length(NodesAvailable) ->
    UpdNodesDeployed = maybe_deploy_nodes(NodesAvailable,
                                          {NodesDeployed,
                                           Config,
                                           InitialVersion}),
    UpdState = State#state{nodes_available=[],
                           nodes_deployed=UpdNodesDeployed},
    {NodesAvailable, UpdState};
reserve(Count, _Versions, Config, State=#state{nodes_available=NodesAvailable,
                                               nodes_deployed=NodesDeployed,
                                               initial_version=InitialVersion}) ->
    {Reserved, UpdNodesAvailable} = lists:split(Count, NodesAvailable),
    UpdNodesDeployed = maybe_deploy_nodes(Reserved,
                                          {NodesDeployed,
                                           Config,
                                           InitialVersion}),
    UpdState = State#state{nodes_available=UpdNodesAvailable,
                           nodes_deployed=UpdNodesDeployed},
    {Reserved, UpdState}.

maybe_deploy_nodes(Nodes, {Nodes, _, _}) ->
    %% All nodes already deployed, move along
    Nodes;
maybe_deploy_nodes(Requested, {Deployed, Version, Config}) ->
    case Deployed -- Requested of
        [] ->
            Deployed;
        NodesToDeploy ->
            _ = rt_harness_util:deploy_nodes(NodesToDeploy, Version, Config),
            lists:sort(Deployed ++ NodesToDeploy)
    end.
