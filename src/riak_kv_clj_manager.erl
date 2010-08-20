-module(riak_kv_clj_manager).

-behaviour(gen_server).

%% API
-export([start_link/1, dispatch/1, blocking_dispatch/1, add_to_manager/0, reload/1, reload/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {tid}).

dispatch(JSCall) ->
    case select_random() of
        no_vms ->
            {error, no_vms};
        Target ->
            JobId = {Target, make_ref()},
            riak_kv_clj_vm:dispatch(Target, self(), JobId, JSCall),
            {ok, JobId}
    end.

blocking_dispatch(JSCall) ->
    case select_random() of
        no_vms ->
            {error, no_vms};
        Target ->
            JobId = {Target, make_ref()},
            riak_kv_clj_vm:blocking_dispatch(Target, JobId, JSCall)
    end.

%% Hack to allow riak-admin to trigger a reload
reload([]) ->
    reload().

reload() ->
    gen_server:call(?MODULE, reload_all_vm).

add_to_manager() ->
    gen_server:cast(?MODULE, {add_child, self()}).

start_link(ChildCount) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [ChildCount], []).

init([ChildCount]) ->
    Tid = ets:new(?MODULE, [named_table]),
    start_children(ChildCount),
    {ok, #state{tid=Tid}}.

handle_call(reload_all_vm, _From, #state{tid=Tid}=State) ->
    ets:safe_fixtable(Tid, true),
    reload_children(ets:first(Tid), Tid),
    ets:safe_fixtable(Tid, false),
    riak_kv_vnode:purge_mapcaches(),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast({add_child, ChildPid}, #state{tid=Tid}=State) ->
    erlang:monitor(process, ChildPid),
    ets:insert_new(Tid, {ChildPid}),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, _Type, Pid, _Info}, #state{tid=Tid}=State) ->
    case ets:lookup(Tid, Pid) of
        [] ->
            {noreply, State};
        [{Pid}] ->
            ets:delete(?MODULE, Pid),
            riak_kv_clj_sup:start_clj(self()),
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
start_children(0) ->
    ok;
start_children(Count) ->
    riak_kv_clj_sup:start_clj(self()),
    start_children(Count - 1).

select_random() ->
    case ets:match(?MODULE, {'$1'}) of
        [] ->
            no_vms;
        Members ->
            {T1, T2, T3} = erlang:now(),
            random:seed(T1, T2, T3),
            Pos = pick_pos(erlang:get(?MODULE), length(Members)),
            [Member] = lists:nth(Pos, Members),
            Member
    end.

pick_pos(undefined, Size) ->
    Pos = random:uniform(Size),
    erlang:put(?MODULE, Pos),
    Pos;
pick_pos(OldPos, Size) ->
    case random:uniform(Size) of
        OldPos ->
            pick_pos(OldPos, Size);
        Pos ->
            erlang:put(?MODULE, Pos),
            Pos
    end.

reload_children('$end_of_table', _Tid) ->
    ok;
reload_children(Current, Tid) ->
    riak_kv_clj_vm:reload(Current),
    reload_children(ets:next(Tid, Current), Tid).
