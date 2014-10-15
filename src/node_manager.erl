-module(node_manager).

-behavior(gen_server).

%% API
-export([start_link/2,
         reserve_nodes/3,
         deploy_nodes/4,
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

start_link(Nodes, VersionMap) ->
    Args = [Nodes, VersionMap],
    gen_server:start_link({local, ?MODULE}, Args, []).

-spec reserve_nodes(pos_integer(), [string()], function()) -> ok.
reserve_nodes(NodeCount, Versions, NotifyFun) ->
    gen_server:cast(?MODULE, {reserve_nodes, NodeCount, Versions, NotifyFun}).

-spec deploy_nodes([string()], string(), term(), function()) -> ok.
deploy_nodes(Nodes, Version, Config, NotifyFun) ->
  gen_server:cast(?MODULE, {deploy_nodes, Nodes, Version, Config, NotifyFun}).

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
init([Nodes, VersionMap]) ->
    SortedNodes = lists:sort(Nodes),
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
    {Result, UpdState} =
        reserve(Count, Versions, State),
    NotifyFun(Result),
    {noreply, UpdState};
handle_cast({deploy_nodes, Nodes, Version, Config, NotifyFun}, State) ->
    %% {Result, UpdState} =
    %%     reserve(Count, Versions, State),
    Result = deploy_nodes(Nodes, Version, Config),
    NotifyFun({nodes_deployed, Result}),
    {noreply, State};
handle_cast({return_nodes, Nodes}, State) ->
    %% Stop nodes and clean data dirs so they are ready for next use.
    [stop_and_clean(Node, State#state.initial_version) || Node <- Nodes],
    NodesAvailable = State#state.nodes_available,
    NodesNowAvailable = lists:merge(lists:sort(Nodes), NodesAvailable),
    {noreply, State#state{nodes_available=NodesNowAvailable}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Stop and reset all deployed nodes
    stop_and_clean(State#state.nodes_deployed, State#state.initial_version),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

stop_and_clean(Node, Version) ->
    rt_node:stop_and_wait(Node),
    rt_node:clean_data_dir(Node, Version).

%% stop_clean_start(Node, Version) ->
%%     stop_and_clean(Node, Version),
%%     rt_node:start(Node, Version).

reserve(Count, _Versions, State=#state{nodes_available=NodesAvailable})
  when Count > length(NodesAvailable) ->
    {not_enough_nodes, State};
reserve(Count, Versions, State=#state{nodes_available=NodesAvailable,
                                      nodes_deployed=NodesDeployed,
                                      version_map=VersionMap})
  when Count =:= length(NodesAvailable) ->
        case versions_available(Count, Versions, VersionMap) of
            true ->
                UpdNodesDeployed = lists:sort(NodesDeployed ++ NodesAvailable),
                {NodesAvailable, State#state{nodes_available=[],
                                             nodes_deployed=UpdNodesDeployed}};
            false ->
                {insufficient_versions_available, State}
        end;
reserve(Count, Versions, State=#state{nodes_available=NodesAvailable,
                                      nodes_deployed=NodesDeployed,
                                      version_map=VersionMap}) ->
        case versions_available(Count, Versions, VersionMap) of
            true ->
                {Reserved, UpdNodesAvailable} = lists:split(Count, NodesAvailable),
                UpdNodesDeployed = lists:sort(NodesDeployed ++ Reserved),
                UpdState = State#state{nodes_available=UpdNodesAvailable,
                                       nodes_deployed=UpdNodesDeployed},
                {Reserved, UpdState};
            false ->
                {insufficient_versions_available, State}
        end.

versions_available(Count, Versions, VersionMap) ->
    lists:all(version_available_fun(Count, VersionMap), Versions).

version_available_fun(Count, VersionMap) ->
    fun(Version) ->
            case lists:keyfind(Version, 1, VersionMap) of
                {Version, VersionNodes} when length(VersionNodes) >= Count ->
                    true;
                {Version, _} ->
                    false;
                false ->
                    false
            end
    end.

deploy_nodes(Nodes, Version, Config) ->
    rt_harness_util:deploy_nodes(Nodes, Version, Config).


%% maybe_deploy_nodes(Nodes, {Nodes, _, _}) ->
%%     %% All nodes already deployed, move along
%%     Nodes;
%% maybe_deploy_nodes(Requested, {Deployed, Version, Config}) ->
%%     case Deployed -- Requested of
%%         [] ->
%%             Deployed;
%%         NodesToDeploy ->
%%             _ = rt_harness_util:deploy_nodes(NodesToDeploy, Version, Config),
%%             lists:sort(Deployed ++ NodesToDeploy)
%%     end.
