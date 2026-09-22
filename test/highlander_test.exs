defmodule HighlanderTest do
  use ExUnit.Case
  doctest Highlander

  test "runs two processes" do
    test_pid = self()

    child_spec = %{
      start:
        {Task, :start_link,
         [
           fn ->
             send(test_pid, :hello)
             Process.sleep(1000)
           end
         ]},
      restart: :transient
    }

    Supervisor.start_link(
      [
        {Highlander, Map.put(child_spec, :id, :one)},
        {Highlander, Map.put(child_spec, :id, :two)}
      ],
      strategy: :one_for_one
    )

    assert_receive(:hello)
    assert_receive(:hello)
  end

  test "runs only one process" do
    test_pid = self()

    child_spec = %{
      start:
        {Task, :start_link,
         [
           fn ->
             send(test_pid, :hello)
             Process.sleep(1000)
           end
         ]},
      restart: :transient
    }

    Supervisor.start_link(
      [
        {Highlander, Map.put(child_spec, :id, :one)}
      ],
      strategy: :one_for_one
    )

    Supervisor.start_link(
      [
        {Highlander, Map.put(child_spec, :id, :one)}
      ],
      strategy: :one_for_one
    )

    assert_receive(:hello)
    refute_receive(:hello)
  end

  test "takes over when one process dies" do
    test_pid = self()

    child_spec = %{
      start:
        {Task, :start_link,
         [
           fn ->
             send(test_pid, :hello)
             Process.sleep(1000)
           end
         ]},
      restart: :transient
    }

    {:ok, pid1} =
      Supervisor.start_link(
        [
          {Highlander, Map.put(child_spec, :id, :one)}
        ],
        strategy: :one_for_one
      )

    {:ok, pid2} =
      Supervisor.start_link(
        [
          {Highlander, Map.put(child_spec, :id, :one)}
        ],
        strategy: :one_for_one
      )

    assert_receive(:hello)
    refute_receive(:hello)

    Supervisor.stop(pid1)

    assert_receive(:hello)
    refute_receive(:hello)
  end

  test "does not crash on a name conflict exit signal when it never became the owner" do
    global_name = :non_owner_name_conflict

    # Simulate another node/process already owning the global name, so that
    # when our Highlander process registers it takes the :no branch and ends
    # up monitoring the existing owner instead of starting its own child
    # supervisor (i.e. its state never gets a :pid key).
    fake_owner = spawn(fn -> Process.sleep(:infinity) end)
    :yes = :global.register_name({Highlander, global_name}, fake_owner)

    child_spec = %{
      id: global_name,
      start: {Task, :start_link, [fn -> Process.sleep(:infinity) end]}
    }

    # Use a plain (non-linked, unrelated) process as the caller, not the
    # test process itself. `start_link` links `pid` to the test process as
    # its parent, and GenServer treats {:EXIT, Parent, Reason} specially
    # (it terminates immediately, matching whatever Reason is given, without
    # ever reaching handle_info)
    {:ok, pid} = GenServer.start_link(Highlander, child_spec)
    unrelated_pid = spawn(fn -> Process.sleep(:infinity) end)

    refute Map.has_key?(:sys.get_state(pid), :pid)

    send(pid, {:EXIT, unrelated_pid, :name_conflict})

    assert :sys.get_state(pid) |> is_map()
    assert Process.alive?(pid)

    :global.unregister_name(global_name)
    Process.exit(fake_owner, :kill)
    Process.exit(pid, :kill)
  end

  test "owner recovers in place (does not get restarted) when it loses a name_conflict" do
    Process.flag(:trap_exit, true)
    test_pid = self()
    global_name = :owner_recovers_in_place

    child_spec = %{
      id: global_name,
      start:
        {Task, :start_link,
         [
           fn ->
             send(test_pid, :hello)
             Process.sleep(:infinity)
           end
         ]}
    }

    # Run the owner under a real, default (:permanent) supervisor, exactly
    # like the production supervisor this bug was found under.
    {:ok, sup} = Supervisor.start_link([{Highlander, child_spec}], strategy: :one_for_one)
    [{_, owner, _, _}] = Supervisor.which_children(sup)
    assert_receive(:hello)
    assert %{pid: _} = :sys.get_state(owner)

    unrelated_pid = spawn(fn -> Process.sleep(:infinity) end)
    send(owner, {:EXIT, unrelated_pid, :name_conflict})
    Process.sleep(50)

    # The critical assertion: this must be the *same* pid. Previously the
    # owner clause called `{:stop, {:shutdown, :name_conflict}, state}`,
    # which caused its `:permanent` parent supervisor to restart it here.
    # Under real production load (multiple singletons all losing a conflict
    # at the same instant on every node join), enough simultaneous restarts
    # exceeded the supervisor's restart intensity and took the whole
    # supervisor - and the entire BEAM node - down with it.
    [{_, owner_after, _, _}] = Supervisor.which_children(sup)

    assert owner == owner_after,
           "owner should recover in place, not be restarted by its supervisor"

    state_after = :sys.get_state(owner)
    refute Map.has_key?(state_after, :pid), "owner should have transitioned to monitor state"
    assert Map.has_key?(state_after, :ref)
  end

  test "does not crash on the self-inflicted :shutdown EXIT from its own Supervisor.stop/2 call" do
    Process.flag(:trap_exit, true)
    test_pid = self()
    global_name = :owner_shutdown_selfsignal

    child_spec = %{
      id: global_name,
      start:
        {Task, :start_link,
         [
           fn ->
             send(test_pid, :hello)
             Process.sleep(:infinity)
           end
         ]}
    }

    {:ok, owner} = GenServer.start_link(Highlander, child_spec)
    assert_receive(:hello)
    assert %{pid: _} = :sys.get_state(owner)

    unrelated_pid = spawn(fn -> Process.sleep(:infinity) end)
    send(owner, {:EXIT, unrelated_pid, :name_conflict})

    # Give time for both messages to be processed: the :name_conflict
    # itself, and the resulting stray {:EXIT, old_local_supervisor_pid,
    # :shutdown} that arrives right after Supervisor.stop/2 returns (since
    # the owner is linked to its own local supervisor and traps exits).
    Process.sleep(50)

    assert Process.alive?(owner),
           "should have absorbed the stray :shutdown EXIT, not crashed"
  end

  test "four singletons under one supervisor all losing a conflict simultaneously does not exceed restart intensity" do
    Process.flag(:trap_exit, true)
    test_pid = self()

    make_spec = fn id ->
      %{
        id: id,
        start:
          {Task, :start_link,
           [
             fn ->
               send(test_pid, {:hello, id})
               Process.sleep(:infinity)
             end
           ]}
      }
    end

    children = for id <- [:s1, :s2, :s3, :s4], do: {Highlander, make_spec.(id)}

    # Default restart intensity: max_restarts: 3, max_seconds: 5 - same as
    # the production supervisor this bug was found under.
    {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one)

    for _ <- 1..4, do: assert_receive({:hello, _})

    owners = Supervisor.which_children(sup) |> Enum.map(fn {_, pid, _, _} -> pid end)
    unrelated_pid = spawn(fn -> Process.sleep(:infinity) end)

    # Simulate all four singletons losing a name-registration race at the
    # same instant - exactly what happens on every real node join/deploy.
    for pid <- owners, do: send(pid, {:EXIT, unrelated_pid, :name_conflict})
    Process.sleep(100)

    assert Process.alive?(sup),
           "supervisor should survive four simultaneous conflicts without exceeding its restart intensity"

    owners_after = Supervisor.which_children(sup) |> Enum.map(fn {_, pid, _, _} -> pid end)

    assert Enum.sort(owners) == Enum.sort(owners_after),
           "no child should have been restarted"
  end

  test "accepts {module, arg} child_child_spec" do
    test_pid = self()

    Supervisor.start_link(
      [
        {Highlander,
         {Task,
          fn ->
            send(test_pid, :hello)
            Process.sleep(1000)
          end}}
      ],
      strategy: :one_for_one
    )

    assert_receive(:hello)
  end
end
