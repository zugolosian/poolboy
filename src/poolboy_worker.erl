%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy_worker).

-callback start_link(WorkerArgs) -> {ok, Pid} |
                                    {error, {already_started, Pid}} |
                                    {error, Reason} when
    WorkerArgs :: proplists:proplist(),
    Pid        :: pid(),
    Reason     :: term().

-callback worker_checkin(Pid, Reason) -> keep | kill when
    Pid :: pid(),
    Reason :: normal | owner_death.

-optional_callbacks([worker_checkin/2]).
