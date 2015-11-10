-module(eccTemp).
-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%-define(SERVER, ?MODULE).

-define(DATA, data).
-define(KV, kvtable).

-record(state, {storepid, clientpid}).

%%% Client API
start_link(ClientPid) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [ClientPid], []).


%%% Server functions
init([ClientPid]) ->
    {ok,StorePid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    State = #state{storepid = StorePid, clientpid = ClientPid}, 
    out("init - StorePid saved to state record ~p~n ClientPid: ~p ", [State#state.storepid, State#state.clientpid]),
    out("Client API: put(Key, Value) ~n get(Key, Version)"),
    {ok, State}. 

handle_call({put, Key, Value}, _From, State) ->
    out("receive get put call"),
    StorePid = State#state.storepid,
    Res = put_data_to_riak(StorePid, Key, Value), 
    {reply, Res, State};


%%should be handle cast when receiving from remote
handle_call({put, Key, Value, Version, Dependencies}, _From, State) ->
    out("receive get put - Dependencies call"),
    StorePid = State#state.storepid, 
    ClientPid = State#state.clientpid,
    {FromPid, _} = _From, 
    out("_From: ~p", [FromPid]),
    case FromPid of 
        ClientPid -> 
            %out("Receved put cal from local client"),
            %Res = put_data_to_riak_local(StorePid, Key, Value);
            out("Receved put cal from remote client -test reversed case"),
            Res = put_data_to_riak_remote(StorePid, Key, Value, Version, Dependencies);
        _ -> 
            out("Receved put cal from local client-test reversed case"),
            Res = put_data_to_riak_local(StorePid, Key, Value)
            %out("Receved put cal from remote client"),
            %Res = put_data_to_riak_remote(StorePid, Key, Value, Version, Dependencies)
    end,
    {reply, Res, State};

handle_call({get, Key}, _From, State) ->
    out("receive get key call"),
    StorePid = State#state.storepid,
    Res = get_data_from_riak(StorePid, Key),
    {reply, Res, State};

handle_call({deletekey, Key}, _From, State) ->
    StorePid = State#state.storepid,
    Res = delete_data_from_riak(StorePid, Key),
    {reply, Res, State};

handle_call({getkeyversion, Key}, _From, State) ->
    StorePid = State#state.storepid,
    Res = get_version_of_key(StorePid, Key), 
    {reply, Res, State};


handle_call({listkeys, Bucket}, _From, State) ->
    StorePid = State#state.storepid,
    case Bucket of 
        data -> 
            Res = list_keys_from_riak(StorePid, ?DATA);
        kvtable -> 
            Res = list_keys_from_riak(StorePid, ?KV);
        _ ->
            Res = not_available
    end,
    {reply, Res, State}.

handle_cast({ok, []}, State) ->
    {noreply, State}.

handle_info(Msg, State) ->
    out("Unexpected message: ~p~n",[Msg]),
    {noreply, State}.

terminate(normal, State) ->
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    %% No change planned. The function is there for the behaviour,
    %% but will not be used. Only a version on the next
    {ok, State}. 


%%% Private functions

get_data_from_riak(StorePid, Key) -> 
    BinBucket = term_to_binary({?DATA}),
    BinKey = term_to_binary({Key}),
    case riakc_pb_socket:get(StorePid, BinBucket, BinKey) of
        {ok, FetchDatas} -> 
            {Version, Value} = binary_to_term(riakc_obj:get_value(FetchDatas)),
             {ok, Key, Version, Value};
        {error, notfound} -> 
            {ok, notfound}
    end. 

put_data_to_riak_local(StorePid, Key, Value) -> 
    put_data_to_riak(StorePid, Key, Value).

put_data_to_riak(StorePid, Key, Value ) -> 
    %out("start put data to riak~n"),
    Version = get_version_of_key(StorePid, Key),
    NewVer = Version + 1, 
    %out("got newversion!!! ~n"),
    case do_put_data_to_riak(StorePid, Key, NewVer, Value) of
        ok -> 
            %out("start put kv to riak 1~n"),
            Res1 = do_put_kv_to_riak(StorePid, Key, NewVer);
        {ok, _} ->
            %out("start put kv to riak 2~n"),
            Res1 = do_put_kv_to_riak(StorePid, Key, NewVer);    
        _ -> 
            %out("not_ok put kv to riak~n"),
            Res1 = not_ok
    end,

    case Res1 of 
        ok -> 
            {ok, Key, NewVer};
        {ok, _} -> 
            {ok, Key, NewVer};
        _ -> 
            {error}
    end.



put_data_to_riak_remote(StorePid, Key, Value, Version, Dependencies) -> 
    
    out("remote put all with {k,v,ver}: {~p,~p,~p} and dependencies:", [Key, Value, Version]),
    lists:map(fun({X, Y}) -> out("{~p,~p}", [X,Y]) end, Dependencies), 

    %check_version
    case is_newer_version(StorePid, Key, Version) of 
        true -> 
            out("New version of key. Start depencency check"),
            depencencies_check(StorePid, Key, Value, Version, Dependencies);
        _ ->    
            out("Old version of key. Discard."),
            {ok, discard}
    end.  

put_data_to_riak_remote(StorePid, Key, Value, Version) -> 
    %out("start put data to riak~n"),
    NewVer = Version, 
    %out("got newversion!!! ~n"),
    case do_put_data_to_riak(StorePid, Key, NewVer, Value) of
        ok -> 
            %out("start put kv to riak 1~n"),
            Res1 = do_put_kv_to_riak(StorePid, Key, NewVer);
        {ok, _} ->
            %out("start put kv to riak 2~n"),
            Res1 = do_put_kv_to_riak(StorePid, Key, NewVer);    
        _ -> 
            %out("not_ok put kv to riak~n"),
            Res1 = not_ok
    end,

    case Res1 of 
        ok -> 
            {ok, Key, NewVer};
        {ok, _} -> 
            {ok, Key, NewVer};
        _ -> 
            {error}
    end.

put_data_to_buffer(Key, Value, Version, Dependencies) -> 
    out("Put data to buffer").

depencencies_check(StorePid, Key, Value, Version, Dependencies) -> 
    out("Start depencency check"),
    case is_depencency_satisfied(StorePid, Dependencies) of 
        true -> 
            out("Dependency check: succeeded"),
            put_data_to_riak_remote(StorePid, Key, Value, Version);
        _ -> 
            out("Dependency check: not succeed"),
            put_data_to_buffer(Key, Value, Version, Dependencies)
    end. 

is_newer_version(StorePid, Key, Version) -> 
    RiakVersion = get_version_of_key(StorePid, Key),
    if 
        Version > RiakVersion -> true;
        true -> false
    end. 


%% foreach key in dep_lists that satisfied, drop it. If all keys satisfied, L return [] 
%% in case of too many keys, will change to foreach each and break immediately when 
%% a key that not satisfied is found
is_depencency_satisfied(StorePid, Dependencies) -> 
    out("Start checking if each depencency satisfied"),
    L = lists:dropwhile(fun(X) -> check_each_dependency(StorePid, X) end, Dependencies),
    if 
        length(L) =:= 0 -> 
            out("Summary: all dependencies satisfied"),
            true; 
        true -> 
            out("Summary: some dependencies not satisfied"),
            false
    end. 

check_each_dependency(StorePid, Depencency) -> 
    {K, V} = Depencency, 
    out("Start checking depencency key satisfied: ~p", [K]),   
    RiakVersion = get_version_of_key(StorePid, K),
    if
        V =< RiakVersion -> 
            out("version for depencency key: ~p satisfied", [K]),
            true;
        RiakVersion < 0 -> 
            out("Riak has not key: ~p yet, not satisfied", [K]),  
            false;
        true -> 
            out("version for depencency key: ~p not satisfied", [K]),
            false
    end.


do_put_data_to_riak(StorePid, Key, NewVer, NewValue ) -> 
    BinBucket = term_to_binary({?DATA}),
    BinKey = term_to_binary({Key}),
    VerVal = {NewVer, NewValue},
    Object = riakc_obj:new(BinBucket, BinKey, VerVal),
    riakc_pb_socket:put(StorePid, Object).  

do_put_kv_to_riak(StorePid, Key, Ver) -> 
    BinBucket = term_to_binary({?KV}),
    BinKey = term_to_binary({Key}),
    Object = riakc_obj:new(BinBucket, BinKey, Ver),
    riakc_pb_socket:put(StorePid, Object).   




delete_data_from_riak(StorePid, Key) -> 
    BinBucketData = term_to_binary({?DATA}),
    BinBucketKV = term_to_binary({?KV}),
    BinKey = term_to_binary({Key}),
    riakc_pb_socket:delete(StorePid, BinBucketData, BinKey),
    riakc_pb_socket:delete(StorePid, BinBucketKV, BinKey).

get_version_of_key(StorePid, Key) -> 
     %out("start get version of key ~n"),
     BinBucket = term_to_binary({?DATA}),
    BinKey = term_to_binary({Key}),
    case riakc_pb_socket:get(StorePid, BinBucket, BinKey) of
        {ok, FetchDatas} -> 
            {Version, _} = binary_to_term(riakc_obj:get_value(FetchDatas)),
            Version;
        {error, notfound} -> 
            -1     
    end. 

list_keys_from_riak(StorePid, Bucket) -> 
    BinBucket = term_to_binary({Bucket}),
    Res = riakc_pb_socket:list_keys(StorePid, BinBucket),
    %%TODO: catch the case RiackC returns binary instead of term - done by make alwasy return binary 
    {ok, BinKeyList} = Res,
    KeyList = lists:map(fun(X) -> binary_to_term(X) end, BinKeyList),
    KeyList.



%% Some helper methods.
out(Format) ->
  out(Format, []).
out(Format, Data) ->
  io:format("~p:~p: ~s~n", [?MODULE, self(), io_lib:format(Format, Data)]).


%just in case 
%save_pid_to_state(Pid) -> 
%    gen_server:call(Pid, {savepid, Pid}).

%%handle_call({savepid, Pid}, _From, State) -> 
%%    NewState = State#state{pid = Pid},
%%    out("savepid - state record now is ~p and ~p", [NewState#state.storepid, NewState#state.pid]),
%%    {reply, ok, NewState};  


%%This function is to backup 
%handle_call({put, Key, Value}, _From, State) ->
    %StorePid = State#state.storepid,
    
    %%% Backup way
    %Res1 = get_version_of_key(StorePid, Key),
    %NewVer = Res1 + 1, 
    %Res = put_data_to_riak1(StorePid, Key, NewVer, Value), 
    %{reply, Res, State};


%put_data_to_riak1(StorePid, Key, NewVer, NewValue ) -> 
%    io:format("start put data to riak"),
%    do_put_data_to_riak(StorePid, Key, NewVer, NewValue).  