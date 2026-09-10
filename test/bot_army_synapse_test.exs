defmodule BotArmySynapseTest do
  use ExUnit.Case, async: true

  describe "version/0" do
    test "returns the mix project version" do
      assert BotArmySynapse.version() == Mix.Project.config()[:version]
    end
  end

  describe "extract_payload/1" do
    test "passes maps through unchanged" do
      payload = %{"key" => "value"}
      assert BotArmySynapse.extract_payload(payload) == payload
    end

    test "returns non-map data as-is" do
      assert BotArmySynapse.extract_payload("raw") == "raw"
    end
  end

  describe "build_envelope/2" do
    test "wraps a payload with the fields the Decoder requires" do
      envelope = BotArmySynapse.build_envelope("synapse.test.event", %{"x" => 1})

      assert envelope["event"] == "synapse.test.event"
      assert envelope["payload"] == %{"x" => 1, "tenant_id" => envelope["tenant_id"]}
      assert is_binary(envelope["event_id"])
      assert is_binary(envelope["timestamp"])
      assert envelope["source"] == "bot_army_synapse"
      assert envelope["schema_version"] == "1.0"
    end
  end
end
