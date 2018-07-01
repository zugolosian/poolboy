%%%-------------------------------------------------------------------
%%% @author davidleach
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Jun 2018 11:04 AM
%%%-------------------------------------------------------------------
-module(poolboy_queue).
-author("davidleach").

%% API
-export([new/0,put/3,get/1,get/2,get_with_strategy/2,get_r/1,get_r/2,peek/1,peek_r/1,drop/1,drop_r/1,length/1,lookup_by_value/2,delete/2,delete_by_value/2,to_list/1,get_by_value/2]).

new() ->
    gb_trees:empty().

put(Priority, Item, Queue) ->
    gb_trees:insert(Priority, Item, Queue).

get(Queue) ->
    case gb_trees:is_empty(Queue) of
        true ->
            {empty};
        false ->
            poolboy_queue:get(Queue, not_empty)
    end.

get(Queue, not_empty) ->
    gb_trees:take_smallest(Queue).

get_r(Queue) ->
    case gb_trees:is_empty(Queue) of
        true ->
            {empty};
        false ->
            poolboy_queue:get_r(Queue, not_empty)
    end.

get_r(Queue, not_empty) ->
    gb_trees:take_largest(Queue).

get_with_strategy(Queue, lifo) ->
    poolboy_queue:get_r(Queue);
get_with_strategy(Queue, fifo) ->
    poolboy_queue:get(Queue).

peek(Queue) ->
    case gb_trees:is_empty(Queue) of
        true ->
            {empty, Queue};
        false ->
            gb_trees:smallest(Queue)
    end.

peek_r(Queue) ->
    case gb_trees:is_empty(Queue) of
        true ->
            {empty, Queue};
        false ->
            gb_trees:largest(Queue)
    end.

delete(Key, Queue) ->
    case gb_trees:is_defined(Key, Queue) of
        true ->
            gb_trees:delete(Key, Queue);
        false ->
            Queue
    end.

delete_by_value(Value, Queue) ->
    case lookup_by_value(Value, Queue) of
        {Key, _Value} ->
            delete(Key, Queue);
        {none} ->
            Queue
    end.

drop(Queue) ->
    {_Key, _Value, Queue2} = poolboy_queue:get(Queue),
    Queue2.

drop_r(Queue) ->
    {_Key, _Value, Queue2} = poolboy_queue:get_r(Queue),
    Queue2.

length(Queue) ->
    gb_trees:size(Queue).

to_list(Queue) ->
    gb_trees:to_list(Queue).

get_by_value(Value, Queue) ->
    case lookup_by_value(Value, Queue) of
        {Key, _Value} ->
            {Value, NewQueue} = gb_trees:take(Key, Queue),
            {{Key, Value}, NewQueue};
        {none} ->
            {{none}, Queue}
    end.

lookup_by_value(Value, Queue) ->
    Iterable = gb_trees:iterator(Queue),
    lookup_by_value1(Value, Iterable).
lookup_by_value1(Value, [{Key,Item, _, _}|_Rest]) when Value == Item ->
    {Key, Item};
lookup_by_value1(Value, [_|Rest]) ->
    lookup_by_value1(Value, Rest);
lookup_by_value1(_Value, []) ->
    {none}.




