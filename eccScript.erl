c(eccSimple).
rr(eccSimple).
{ok,StorePid} = riakc_pb_socket:start_link("127.0.0.1", 8087).
{ok, Pid } = eccSimple:start_link(StorePid).
eccSimple:put(Pid, "Viet", ["Good"]).
eccSimple:get(Pid, "Viet", ).

f().
c(eccClient), c(eccTemp).
{ok, Pid} = eccClient:start_link().
eccClient:put(manuel, assistant, 7, withdependencies).
eccClient:get("Van").
eccClient:put(vanroy, better).
eccClient:list_keys(data).
eccClient:get_key_version(minh).

eccClient:put(mens, prof, 0, withdependencies).

eccClient:get_dependency_list().



c(eccTemp).
rr(eccTemp).
{ok, Pid } = eccTemp:start_link().
eccTemp:get_key_version("Van").
eccTemp:put("Van", ["Good"]).
eccTemp:put("Viet", ["Good"]).
eccTemp:get("Viet" ).
eccTemp:list_keys(data).


BinBucketData = term_to_binary({data}),
BinBucketKV = term_to_binary({kvtable}),
BinKey = term_to_binary({Key}),
riakc_pb_socket:delete(StorePid, BinBucketData, BinKey).
riakc_pb_socket:delete(StorePid, BinBucketKV, BinKey).






{ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8087).
{ok,<0.45.0>}
riakc_pb_socket:ping(Pid).
pong
MyBucket = groceries.
MyKey = mine.
MyValue = "eggs & bacon".

BinMyBucket = term_to_binary(MyBucket).
BinMyKey = term_to_binary(MyKey).
ListMyValue = [MyValue].

Obj1 = riakc_obj:new(BinMyBucket, BinMyKey, ListMyValue).
riakc_pb_socket:put(StorePid, Obj1).
{ok, BinKeyList} = riakc_pb_socket:list_keys(StorePid, BinMyBucket).
BinKeyList.

Object = riakc_obj:new(MyBucket, MyKey, MyValue).
Object = riakc_obj:new(<<"groceries">>, <<"mine">>, <<"eggs & bacon">>).
{riakc_obj,<<"groceries">>,<<"mine">>,undefined,[],
           undefined,<<"eggs & bacon">>}
riakc_pb_socket:put(Pid, Object).
ok
riakc_pb_socket:list_keys(Pid, MyBucket).
riakc_pb_socket:list_keys(Pid, <<"groceries">>).
{ok,[<<"mine">>]}


5> riakc_pb_socket:list_keys(Pid, <<"Data">>).
{ok,[<<131,104,1,97,1>>,
     <<131,104,1,107,0,3,86,97,110>>,
     <<131,104,1,100,0,8,97,110,121,116,104,105,110,103>>,
     <<131,104,1,107,0,4,86,105,101,116>>]}