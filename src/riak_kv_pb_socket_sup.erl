%% -------------------------------------------------------------------
%%
%% riak_kv_pb_socket_sup: supervise riak_kv_pb_socket processes
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc supervise riak_kv_pb_socket processes

-module(riak_kv_pb_socket_sup).
-behaviour(supervisor).
-export([start_link/0, init/1, stop/1]).
-export([start_socket/1]).

start_socket(Socket) -> 
    supervisor:start_child(?MODULE, [Socket]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_S) -> ok.

%% @private
init([]) ->
    {ok, 
     {{simple_one_for_one, 10, 10}, 
      [{undefined,
        {riak_kv_pb_socket, start_link, []},
        temporary, brutal_kill, worker, [riak_kv_pb_socket]}]}}.
