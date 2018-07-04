-module(poolboy_test_worker_checkin_callback).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, worker_checkin/2]).

start_link(_Args) ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, undefined}.

handle_call(die, _From, State) ->
    {stop, {error, died}, dead, State};
handle_call(_Event, _From, State) ->
    {reply, ok, State}.

handle_cast(_Event, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

worker_checkin(_Pid, normal) ->
    keep;
worker_checkin(_Pid, owner_death) ->
    kill.
