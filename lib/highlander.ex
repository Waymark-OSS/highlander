defmodule Highlander do
  @external_resource "README.md"
  @moduledoc @external_resource
             |> File.read!()
             |> String.split("<!-- MDOC !-->")
             |> Enum.fetch!(1)

  use GenServer
  require Logger

  def child_spec(child_child_spec) do
    child_child_spec = Supervisor.child_spec(child_child_spec, [])

    Logger.debug("Starting Highlander with #{inspect(child_child_spec.id)} as uniqueness key")

    %{
      id: child_child_spec.id,
      start: {GenServer, :start_link, [__MODULE__, child_child_spec, []]}
    }
  end

  @impl true
  def init(child_spec) do
    Process.flag(:trap_exit, true)
    {:ok, register(%{child_spec: child_spec})}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _, _}, %{ref: ref} = state) do
    Logger.warning("#{__MODULE__}: handling :DOWN for :process")
    {:noreply, register(state)}
  end

  def handle_info({:EXIT, _pid, :name_conflict}, %{pid: pid} = state) do
    Logger.warning("#{__MODULE__}: handling :EXIT for :name_conflict with pid")
    :ok = Supervisor.stop(pid, :shutdown)
    {:noreply, state |> Map.delete(:pid) |> monitor()}
  end

  # We get here if we never started the process. Otherwise we get a
  # `FunctionClauseError` which eventually causes the entire BEAM process to die
  # if we are not isolating the Highlander processes with a dedicated supervisor
  def handle_info({:EXIT, _pid, :name_conflict}, state) do
    Logger.warning("#{__MODULE__}: handling :EXIT for :name_conflict")
    {:noreply, state}
  end

  # Handle the :shutdown case when the :EXIT bubbles up from the above
  # `Supervisor.stop/2`. Otherwise we will get a `FunctionClauseError` as well.
  # that call returns — absorb it.
  def handle_info({:EXIT, _pid, :shutdown}, state) do
    Logger.warning("#{__MODULE__}: handling :EXIT for :shutdown")
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    Logger.warning("#{__MODULE__}: handling :EXIT for #{reason}")
    {:noreply, state}
  end

  @impl true
  def terminate(reason, %{pid: pid}) do
    Logger.warning("#{__MODULE__}: handling terminate for #{reason}")
    :ok = Supervisor.stop(pid, reason)
  end

  def terminate(_, _), do: nil

  defp name(%{child_spec: %{id: global_name}}) do
    {__MODULE__, global_name}
  end

  defp handle_conflict(_name, pid1, pid2) do
    Process.exit(pid2, :name_conflict)
    pid1
  end

  defp register(state) do
    case :global.register_name(name(state), self(), &handle_conflict/3) do
      :yes -> start(state)
      :no -> monitor(state)
    end
  end

  defp start(state) do
    {:ok, pid} = Supervisor.start_link([state.child_spec], strategy: :one_for_one)
    Map.put(state, :pid, pid)
  end

  defp monitor(state) do
    case :global.whereis_name(name(state)) do
      :undefined ->
        register(state)

      pid ->
        ref = Process.monitor(pid)
        %{child_spec: state.child_spec, ref: ref}
    end
  end
end
