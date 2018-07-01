%%%-------------------------------------------------------------------
%%% @author davidleach
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Jun 2018 11:04 AM
%%%-------------------------------------------------------------------
-module(poolboy_priority_queue).
-author("davidleach").

%% API
-export([new/0,put/3,get/1,get_with_strategy/2,get_r/1,peek/1,length/1,lookup_by_value/2,delete/2,delete_by_value/2,to_list/1]).

-export_type([priority/0]).
-type priority() :: integer() | {integer(), integer()}.
-spec new() -> gb_trees:tree().

new() ->
    gb_trees:empty().

-spec put(Priority :: priority(), Item :: term(), Queue :: gb_trees:tree(_,_)) ->
    gb_trees:tree(_,_).

put(Priority, Item, Queue) ->
    gb_trees:insert(Priority, Item, Queue).

-spec get(gb_trees:tree(_,_)) -> {'empty'} | gb_trees:tree(_,_).
get(Queue) ->
    case gb_trees:is_empty(Queue) of
        false ->
            gb_trees:take_smallest(Queue);
        true ->
            {empty}
    end.

-spec get_r(gb_trees:tree(_,_)) -> {'empty'} | gb_trees:tree(_,_).
get_r(Queue) ->
    case gb_trees:is_empty(Queue) of
        false ->
            gb_trees:take_largest(Queue);
        true ->
            {empty}
    end.

-spec get_with_strategy(Queue :: gb_trees:tree(_,_),'fifo' | 'lifo') ->
    {'empty'} | gb_trees:tree(_,_).

get_with_strategy(Queue, lifo) ->
    poolboy_priority_queue:get_r(Queue);
get_with_strategy(Queue, fifo) ->
    poolboy_priority_queue:get(Queue).

-spec peek(Queue :: gb_trees:tree(_,_)) ->
    {empty, gb_trees:tree()} | {Key :: priority(), Value :: term()}.

peek(Queue) ->
    case gb_trees:is_empty(Queue) of
        false ->
            gb_trees:smallest(Queue);
        true ->
            {empty, Queue}
    end.

-spec delete(_,gb_trees:tree(_,_)) -> gb_trees:tree(_,_).

delete(Key, Queue) ->
    case gb_trees:is_defined(Key, Queue) of
        true ->
            gb_trees:delete(Key, Queue);
        false ->
            Queue
    end.

-spec delete_by_value(_,gb_trees:tree(_,_)) -> gb_trees:tree(_,_).

delete_by_value(Value, Queue) ->
    case lookup_by_value(Value, Queue) of
        {none} ->
            Queue;
        {Key} ->
            delete(Key, Queue)

    end.

-spec length(gb_trees:tree(_,_)) -> non_neg_integer().

length(Queue) ->
    gb_trees:size(Queue).

-spec to_list(gb_trees:tree(_,_)) -> [{_,_}].

to_list(Queue) ->
    gb_trees:to_list(Queue).

-spec lookup_by_value(Values :: pid(), Queue :: gb_trees:tree()) ->

    {none} | {priority()}.
lookup_by_value(Value, Queue) ->
    lookup_by_value_iter(Value, gb_trees:next(gb_trees:iterator(Queue))).

-spec lookup_by_value_iter(Value :: pid(), none | gb_trees:iter()) ->
    {none} | {priority()}.
lookup_by_value_iter(_Value, none) ->
    {none};
lookup_by_value_iter(Value, {Key, Value, _Iterable}) ->
    {Key};
lookup_by_value_iter(Value, {_Key, _Value, Iterable}) ->
    lookup_by_value_iter(Value, gb_trees:next(Iterable)).





