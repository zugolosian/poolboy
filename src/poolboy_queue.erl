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
-export([new/0,put/3,get/1,get/2,get_with_strategy/2,get_r/1,get_r/2,peek/1,peek_r/1,drop/1,drop_r/1,length/1,value_member/2,delete/2,delete_by_value/2,to_list/1]).

new() ->
    gb_trees:empty().

put(MonotonicTime, Item, Queue) ->
    gb_trees:insert(MonotonicTime, Item, Queue).

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
    gb_trees:smallest(Queue).

peek_r(Queue) ->
    gb_trees:largest(Queue).

delete(Key, Queue) ->
    gb_trees:delete(Key, Queue).

delete_by_value(Value, Queue) ->
    case value_member(Value, Queue) of
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

value_member(Value, Queue) ->
    Iterable = gb_trees:to_list(Queue),
    value_member1(Value, Iterable).
value_member1(Value, [{Key,Item}|_Rest]) when Value == Item ->
    {Key, Item};
value_member1(Value, [_|Rest]) ->
    value_member1(Value, Rest);
value_member1(_Value, []) ->
    {none}.




