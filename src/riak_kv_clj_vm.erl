-module(riak_kv_clj_vm).

-behaviour(gen_server).

-define(MAX_ANON_FUNS, 25).

%% API
-export([start_link/1, dispatch/4, blocking_dispatch/3, reload/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {manager, ctx, next_funid=1, anon_funs=[]}).

start_link(Manager) ->
    gen_server:start_link(?MODULE, [Manager], []).

dispatch(VMPid, Requestor, JobId, JSCall) ->
    gen_server:cast(VMPid, {dispatch, Requestor, JobId, JSCall}).

blocking_dispatch(VMPid, JobId, JSCall) ->
    gen_server:call(VMPid, {dispatch, JobId, JSCall}, 10000).

reload(VMPid) ->
    gen_server:cast(VMPid, reload).

init([Manager]) ->
    error_logger:info_msg("Clojure VM starting (~p)~n",
                          [self()]),
    riak_kv_clj_manager:add_to_manager(),
    erlang:monitor(process, Manager),
    {ok, #state{manager=Manager}}.

% Reduce Function Call
handle_call({dispatch, _JobId, {{cljanon, JS}, Reduced, _Arg}}, _From, State) ->
    Reply = {ok, call_clojure(red, Reduced, JS)},
    {reply, Reply, State};
%% Pre-commit hook with named function
% handle_call({dispatch, _JobId, {{jsfun, JS}, Obj}}, _From, #state{ctx=Ctx}=State) ->
%     {reply, invoke_js(Ctx, JS, [riak_object:to_json(Obj)]), State};
handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(reload, State) ->
    % init_context(Ctx),
    error_logger:info_msg("Clojure VM host reloaded (~p)~n", [self()]),
    {noreply, State};

% Map Function Call
handle_cast({dispatch, Requestor, _JobId, {Sender, {map, {cljanon, JS}, _Arg, _Acc},
                                            Value,
                                            _KeyData, _BKey}}, State) ->
    Result = {ok, call_clojure(map, riak_object:get_value(Value), JS)},
    case Result of
        {ok, ReturnValue} ->
            riak_core_vnode:reply(Sender, {mapexec_reply, ReturnValue, Requestor}),
            {noreply, State};
        ErrorResult ->
            riak_core_vnode:reply(Sender, {mapexec_error_noretry, Requestor, ErrorResult}),
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, _Type, Manager, _Info}, #state{manager=Manager}=State) ->
    {stop, normal, State#state{manager=undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    error_logger:info_msg("Clojure VM host stopping (~p)~n", [self()]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

call_clojure(Command, Val, JS) ->
  % io:format("Calling Clojure: ~p ~p ~p~n", [Command, Val, JS]),
  {cljmbox, 'clj2@mrhyde'} ! {{pid, self()}, {command, Command}, {'r-value', Val}, 
  {'clj-map-fn', JS}},
  receive 
    {clj, Msg} -> 
      % io:format("Clojure Reply: ~p~n", [Msg]),
      Msg
  end.

% read_config(Param, Default) ->
%     case app_helper:get_env(riak_kv, Param, 8) of
%         N when is_integer(N) ->
%             N;
%         _ ->
%             Default
%     end.