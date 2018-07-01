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

-spec new() -> gb_trees:tree().

new() ->
    gb_trees:empty().

-spec put(Priority :: integer() | tuple(), Item :: term(), Queue :: gb_trees:tree(_,_)) ->
    gb_trees:tree(_,_).

put(Priority, Item, Queue) ->
    gb_trees:insert(Priority, Item, Queue).

-spec get(gb_trees:tree(_,_)) -> {'empty'} | {_,_,gb_trees:tree(_,_)}.
get(Queue) ->
    case gb_trees:is_empty(Queue) of
        false ->
            gb_trees:take_smallest(Queue);
        true ->
            {empty}
    end.

-spec get_r(gb_trees:tree(_,_)) -> {'empty'} | {_,_,gb_trees:tree(_,_)}.
get_r(Queue) ->
    case gb_trees:is_empty(Queue) of
        false ->
            gb_trees:take_largest(Queue);
        true ->
            {empty}
    end.

-spec get_with_strategy(Queue :: gb_trees:tree(_,_),'fifo' | 'lifo') ->
    {'empty'} | {_,_,gb_trees:tree(_,_)}.

get_with_strategy(Queue, lifo) ->
    poolboy_priority_queue:get_r(Queue);
get_with_strategy(Queue, fifo) ->
    poolboy_priority_queue:get(Queue).

-spec peek(gb_trees:tree(_,_)) -> {_,_}.

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

-spec lookup_by_value(_,'none' | {_,_,_} | gb_trees:tree(_,_)) -> {_}.
lookup_by_value(_Value, none) ->
    {none};
lookup_by_value(Value, {Key, Value, _Iterable}) ->
    {Key};
lookup_by_value(Value, {_Key, _Value, Iterable}) ->
    lookup_by_value(Value, gb_trees:next(Iterable));
lookup_by_value(Value, Queue) ->
    lookup_by_value(Value, gb_trees:next(gb_trees:iterator(Queue))).




