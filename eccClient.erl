-module(eccClient).
-behaviour(gen_server).

-export([start_link/0, put/2, put/4, get/1, list_keys/1, get_key_version/1, delete_key/1, get_client_deplist/0 ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%-define(SERVER, ?MODULE).

-record(state, {eccCorePid, dependencies}).

%%% Client API
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

put(Key, Value) -> 
    gen_server:call(?MODULE, {put, Key, Value}).

put(Key, Value, Version, withdependencies) -> 
    gen_server:call(?MODULE, {put, Key, Value, Version, withdependencies}).

get(Key) -> 
    gen_server:call(?MODULE, {get, Key}).

%%testing, will not expose
list_keys(Bucket) -> 
    gen_server:call(?MODULE, {listkeys, Bucket}).


get_key_version(Key) -> 
    gen_server:call(?MODULE, {getkeyversion, Key}).

delete_key(Key) -> 
    gen_server:call(?MODULE, {deletekey, Key}).

get_client_deplist() -> 
    gen_server:call(?MODULE, {getclientdeplist}).

%%% Server functions
init([]) ->
    {ok, Pid } = eccTemp:start_link(self()),
    %Dependencies = [{vanroy, 0}, {mens, 0}], %%test dependencies
    %Dependencies = [{vanroy, 3}], %%test dependencies
    Dependencies = [],
    State = #state{eccCorePid = Pid, dependencies = Dependencies},

    out("init - eccCorePid saved to state record ~p", [State#state.eccCorePid]),
    out("init - first dependencies: "),
    lists:map(fun({X, Y}) -> out("{~p,~p}", [X,Y]) end, Dependencies),
    lists:flatten(io_lib:format("~p", [Dependencies])),
    
    out("Client API: put(Key, Value), get(Key, Version)"),
    {ok, State}. 

handle_call({put, Key, Value}, _From, State) ->
    EccCorePid = State#state.eccCorePid,

    out("init - list of dependencies now: "),
    Dependencies = State#state.dependencies,
    lists:map(fun({X, Y}) -> out("{~p,~p}", [X,Y]) end, Dependencies),

    Res = gen_server:call(EccCorePid, {put, Key, Value}),

    case Res of 
        {ok, K, Ver} -> 
            out("repopulate client library dependency list"),
            NewDep = [{K, Ver}],
            NewState = #state{eccCorePid = EccCorePid, dependencies = NewDep};
        _ -> 
            NewState = State,
            out("put fail")
    end,
    {reply, Res, NewState};


handle_call({put, Key, Value, Version, withdependencies}, _From, State) ->
    EccCorePid = State#state.eccCorePid,
    Dependencies = State#state.dependencies,
    Res = gen_server:call(EccCorePid, {put, Key, Value, Version, Dependencies}),
    {reply, Res, State};

handle_call({get, Key}, _From, State) ->
    EccCorePid = State#state.eccCorePid,

    out("init - list of dependencies now: "),
    Dependencies = State#state.dependencies,
    lists:map(fun({X, Y}) -> out("{~p,~p}", [X,Y]) end, Dependencies),

    Res = gen_server:call(EccCorePid, {get, Key}),
    case Res of 
        {ok, K, Ver, _} -> 
            out("add this get to client library dependency list"),
            %Dependencies = State#state.dependencies,
            NewDep = [{K, Ver}| Dependencies],
            NewState = #state{eccCorePid = EccCorePid, dependencies = NewDep};
        _ -> 
            NewState = State,
            out("get fail")
    end,
    {reply, Res, NewState};

handle_call({deletekey, Key}, _From, State) ->
    EccCorePid = State#state.eccCorePid,
    Res = gen_server:call(EccCorePid, {deletekey, Key}),
    {reply, Res, State};

handle_call({getkeyversion, Key}, _From, State) ->
    EccCorePid = State#state.eccCorePid,
    Res = gen_server:call(EccCorePid, {getkeyversion, Key}),
    {reply, Res, State};


handle_call({listkeys, Bucket}, _From, State) ->
    EccCorePid = State#state.eccCorePid,
    Res = gen_server:call(EccCorePid, {listkeys, Bucket}),
    {reply, Res, State};

handle_call({getclientdeplist}, _From, State) ->
    EccCorePid = State#state.eccCorePid,
    Dependencies = State#state.dependencies,
    out("current dependencies: "),
    lists:map(fun({X, Y}) -> out("{~p,~p}", [X,Y]) end, Dependencies),
    lists:flatten(io_lib:format("~p", [Dependencies])),
    Res = ok,
    %Res = gen_server:call(EccCorePid, {listkeys, Bucket}),
    {reply, Res, State}.

handle_cast(_Request, State) ->
  {noreply, State}.


handle_info(Msg, State) ->
    {noreply, State}.

terminate(normal, State) ->
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    %% No change planned. The function is there for the behaviour,
    %% but will not be used. Only a version on the next
    {ok, State}. 


%%% Private functions


%% Some helper methods.
out(Format) ->
  out(Format, []).
out(Format, Data) ->
  io:format("~p:~p: ~s~n", [?MODULE, self(), io_lib:format(Format, Data)]).