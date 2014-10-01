-module(node_manager).

-behavior(gen_server).

%% API
-export([start_link/2,
         reserve_nodes/3,
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

start_link(Nodes, VersionMap) ->
    gen_server:start_link({local, ?MODULE}, [Nodes, VersionMap], []).

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
