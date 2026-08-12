defmodule Pigeon.FCMTest do
  use ExUnit.Case
  doctest Pigeon.FCM, import: true
  doctest Pigeon.FCM.Config, import: true
  doctest Pigeon.FCM.Notification, import: true

  alias Pigeon.FCM
  alias Pigeon.FCM.Config
  alias Pigeon.FCM.Notification
  alias Pigeon.HTTP.Request
  alias Pigeon.HTTP.RequestQueue
  require Logger

  @data %{"message" => "Test push"}
  @invalid_project_msg ~r/^attempted to start without valid :project_id/
  @invalid_auth_msg ~r/^attempted to start without valid :auth module/

  defp valid_fcm_reg_id do
    Application.get_env(:pigeon, :test)[:valid_fcm_reg_id]
  end

  # Google closes idle HTTP/2 connections. Mint marks the struct `:closed` once
  # the transport is gone and `{:goaway, ...}` while the server drains; it
  # refuses to open a stream on either, which is what used to raise here.
  defp closed_socket, do: %Mint.HTTP2{state: :closed}
  defp draining_socket, do: %Mint.HTTP2{state: {:goaway, :no_error, ""}}

  # Points the config at a port nothing listens on, so reconnecting fails fast
  # and deterministically instead of reaching fcm.googleapis.com.
  defp unreachable_config do
    {:ok, listener} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(listener)
    :ok = :gen_tcp.close(listener)

    %Config{
      auth: PigeonTest.Goth,
      project_id: "example-project",
      uri: ~c"localhost",
      port: port
    }
  end

  # A connection Mint will actually write a request to: a real socket, pointed
  # at a local listener that never answers. Enough to drive `Mint.HTTP.request`
  # through encoding and transport send without reaching fcm.googleapis.com.
  defp writable_socket do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(listener)

    {:ok, socket} =
      :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    {:ok, _accepted} = :gen_tcp.accept(listener)

    %Mint.HTTP2{
      transport: Mint.Core.Transport.TCP,
      socket: socket,
      mode: :active,
      state: :open,
      scheme: "https",
      hostname: "fcm.googleapis.com",
      authority: "fcm.googleapis.com",
      port: 443
    }
  end

  defp reachable_config do
    %Config{auth: PigeonTest.Goth, project_id: "example-project"}
  end

  defp notification_replying_to(pid) do
    notification = Notification.new({:token, "device-token"}, %{}, @data)
    on_response = fn response -> send(pid, {:responded, response}) end

    %{
      notification
      | __meta__: %{notification.__meta__ | on_response: on_response}
    }
  end

  describe "init/1" do
    test "raises if configured with invalid project" do
      assert_raise(Pigeon.ConfigError, @invalid_project_msg, fn ->
        [project_id: nil, auth: PigeonTest.Goth]
        |> Pigeon.FCM.init()
      end)
    end

    test "raises if configured with invalid auth module" do
      assert_raise(Pigeon.ConfigError, @invalid_auth_msg, fn ->
        [project_id: "example", auth: nil]
        |> Pigeon.FCM.init()
      end)
    end
  end

  describe "handle_push/2" do
    test "when the connection is healthy, queues the request against it" do
      state = %FCM{config: reachable_config(), socket: writable_socket()}

      assert {:noreply, new_state} =
               FCM.handle_push(notification_replying_to(self()), state)

      assert [%Request{notification: %Notification{}}] =
               Map.values(new_state.queue.requests)

      refute_receive {:responded, _notification}, 200
    end

    test "when the connection cannot open another stream, reports the notification as unavailable" do
      socket = %{writable_socket() | open_client_stream_count: 100}
      state = %FCM{config: unreachable_config(), socket: socket}

      assert {:noreply, _new_state} =
               FCM.handle_push(notification_replying_to(self()), state)

      assert_receive {:responded, %Notification{response: :unavailable}}, 1_000
    end

    test "when the connection is dead, reports the notification as unavailable" do
      state = %FCM{config: unreachable_config(), socket: closed_socket()}

      assert {:noreply, ^state} =
               FCM.handle_push(notification_replying_to(self()), state)

      assert_receive {:responded,
                      %Notification{response: :unavailable, error: error}},
                     1_000

      assert error
    end

    test "when the server is draining the connection, reports the notification as unavailable" do
      state = %FCM{config: unreachable_config(), socket: draining_socket()}

      assert {:noreply, ^state} =
               FCM.handle_push(notification_replying_to(self()), state)

      assert_receive {:responded, %Notification{response: :unavailable}}, 1_000
    end

    test "when the connection is dead and nobody is listening, does not raise" do
      state = %FCM{config: unreachable_config(), socket: closed_socket()}
      notification = Notification.new({:token, "device-token"}, %{}, @data)

      assert {:noreply, ^state} = FCM.handle_push(notification, state)
    end

    test "when the connection is dead, leaves the request queue empty" do
      state = %FCM{config: unreachable_config(), socket: closed_socket()}

      assert {:noreply, new_state} =
               FCM.handle_push(notification_replying_to(self()), state)

      assert new_state.queue == RequestQueue.new()
    end
  end

  describe "handle_info/2" do
    test "when a ping finds a dead connection it cannot restore, stops the dispatcher" do
      state = %FCM{config: unreachable_config(), socket: closed_socket()}

      assert {:stop, reason} = FCM.handle_info(:ping, state)
      assert reason
    end

    test "when the connection closed and cannot be restored, stops the dispatcher" do
      state = %FCM{config: unreachable_config(), socket: closed_socket()}

      assert {:stop, reason} =
               FCM.handle_info({:closed, closed_socket()}, state)

      assert reason
    end
  end

  describe "push/1" do
    @describetag :integration

    test "successfully sends a valid push" do
      notification =
        {:token, valid_fcm_reg_id()}
        |> Notification.new(%{}, @data)
        |> PigeonTest.FCM.push()

      assert notification.name
    end

    test "successfully sends a valid push with callback" do
      target = {:token, valid_fcm_reg_id()}
      n = Notification.new(target, %{}, @data)
      pid = self()
      PigeonTest.FCM.push(n, on_response: fn x -> send(pid, x) end)

      assert_receive(n = %Notification{target: ^target}, 5000)
      assert n.name
      assert n.response == :success
    end

    @tag :focus
    test "successfully sends a valid push with a dynamic dispatcher" do
      target = {:token, valid_fcm_reg_id()}
      n = Notification.new(target, %{}, @data)
      pid = self()

      {:ok, dispatcher} =
        Pigeon.Dispatcher.start_link(
          Application.get_env(:pigeon, PigeonTest.FCM)
        )

      Pigeon.push(dispatcher, n, on_response: fn x -> send(pid, x) end)

      assert_receive(n = %Notification{target: ^target}, 5000)
      assert n.name
      assert n.response == :success
    end

    test "returns an error on pushing with a bad registration_id" do
      target = {:token, "bad_reg_id"}
      n = Notification.new(target, %{}, @data)
      pid = self()
      PigeonTest.FCM.push(n, on_response: fn x -> send(pid, x) end)

      assert_receive(n = %Notification{target: ^target}, 5000)
      assert n.error
      refute n.name
      assert n.response == :invalid_argument
    end

    test "responds :not_started if dispatcher not started" do
      target = {:token, valid_fcm_reg_id()}
      n = Notification.new(target, %{}, @data)
      pid = self()

      Pigeon.push(PigeonTest.NotStarted, n,
        on_response: fn x -> send(pid, x) end
      )

      assert_receive(n = %Notification{target: ^target}, 5000)
      refute n.name
      assert n.response == :not_started
    end
  end
end
