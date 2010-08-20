-module(riak_kv_clj_sup).
-behaviour(supervisor).
-export([start_link/0, init/1, stop/1]).
-export([start_clj/1]).

start_clj(Manager) when is_pid(Manager) ->
    supervisor:start_child(?MODULE, [Manager]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_S) -> ok.

%% @private
init([]) ->
    {ok,
     {{simple_one_for_one, 10, 10},
      [{undefined,
        {riak_kv_clj_vm, start_link, []},
        temporary, 2000, worker, [riak_kv_clj_vm]}]}}.