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
