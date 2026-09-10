defmodule BotArmySynapse.GossipSchedulerTest do
  use ExUnit.Case, async: false

  test "starts and initializes empty gossip state" do
    start_supervised =
      case GenServer.start_link(BotArmySynapse.GossipScheduler, []) do
        {:ok, pid} -> {:ok, pid}
        {:error, {:already_started, pid}} -> {:ok, pid}
      end

    assert Process.alive?(elem(start_supervised, 1))

    state = :sys.get_state(BotArmySynapse.GossipScheduler)
    assert is_map(state)
    assert state.last_gossip_at == nil

    on_exit(fn ->
      if pid = Process.whereis(BotArmySynapse.GossipScheduler) do
        Process.exit(pid, :kill)
      end
    end)
  end
end
