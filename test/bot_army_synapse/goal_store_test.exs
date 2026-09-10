defmodule BotArmySynapse.GoalStoreTest do
  use ExUnit.Case, async: false

  # GoalStore's init attempts a GTD sync (pulse cache -> NATS fallback). In the test
  # environment neither is available, so init ends in the graceful empty-cache path.
  setup do
    on_exit(fn ->
      if pid = Process.whereis(BotArmySynapse.GoalStore) do
        Process.exit(pid, :kill)
      end
    end)
    :ok
  end

  test "starts and serves an empty cache when no GTD source is reachable" do
    start =
      case GenServer.start_link(BotArmySynapse.GoalStore, []) do
        {:ok, pid} -> {:ok, pid}
        {:error, {:already_started, pid}} -> {:ok, pid}
      end

    assert {:ok, pid} = start
    assert Process.alive?(pid)

    # rescue-wrapped API degrades to nil/empty rather than crashing callers
    assert BotArmySynapse.GoalStore.list_goals(:active) == []
    assert BotArmySynapse.GoalStore.list_goals(:all) == []
    assert BotArmySynapse.GoalStore.get_goal("nonexistent") == nil
    assert BotArmySynapse.GoalStore.get_goal_by_name("nonexistent") == nil
  end

  test "record_decision on an unknown project is a no-op" do
    {:ok, _} = GenServer.start_link(BotArmySynapse.GoalStore, [])

    assert :ok == GenServer.cast(BotArmySynapse.GoalStore, {:record_decision, "p1", "summary"})
    # give the cast a beat, then confirm the unknown project did not materialize
    Process.sleep(50)
    assert BotArmySynapse.GoalStore.get_goal("p1") == nil
  end
end
