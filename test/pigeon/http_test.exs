defmodule Pigeon.HTTPTest do
  use ExUnit.Case, async: true

  alias Pigeon.HTTP
  alias Pigeon.HTTP.Request
  alias Pigeon.HTTP.RequestQueue

  # Google closes idle HTTP/2 connections. Mint records that on the struct --
  # `:closed` once the transport is gone, `{:goaway, ...}` while the server is
  # draining -- and refuses to open new streams on either. Building the structs
  # directly reproduces both without a socket.
  defp closed_socket, do: %Mint.HTTP2{state: :closed}
  defp draining_socket, do: %Mint.HTTP2{state: {:goaway, :no_error, ""}}
  defp open_socket, do: %Mint.HTTP2{state: :open}

  defp state(socket, queue \\ RequestQueue.new()) do
    %{socket: socket, queue: queue}
  end

  describe "handle_stream/3" do
    test "when the message belongs to another socket, leaves the state untouched" do
      state = state(open_socket())

      assert {:noreply, ^state} =
               HTTP.handle_stream(:unknown, state, fn _request -> :ok end)

      refute_received {:closed, _socket}
    end

    test "when a response completes, hands the request to the handler" do
      ref = make_ref()

      queue =
        RequestQueue.add(RequestQueue.new(), ref, %{token: "device-token"})

      socket = open_socket()
      test_pid = self()

      responses = [
        {:status, ref, 200},
        {:data, ref, ~s({"name":"projects/example/messages/1"})},
        {:done, ref}
      ]

      assert {:noreply, new_state} =
               HTTP.handle_stream(
                 {:ok, socket, responses},
                 state(socket, queue),
                 fn request -> send(test_pid, {:handled, request}) end
               )

      assert_received {:handled,
                       %Request{
                         notification: %{token: "device-token"},
                         body: ~s({"name":"projects/example/messages/1"}),
                         status: 200,
                         done?: true
                       }}

      assert new_state.queue == RequestQueue.new()
      refute_received {:closed, _socket}
    end

    test "when a response is still streaming, keeps it queued and calls no handler" do
      ref = make_ref()

      queue =
        RequestQueue.add(RequestQueue.new(), ref, %{token: "device-token"})

      socket = open_socket()

      assert {:noreply, new_state} =
               HTTP.handle_stream(
                 {:ok, socket, [{:status, ref, 200}]},
                 state(socket, queue),
                 fn _request ->
                   flunk("handler ran before the response was done")
                 end
               )

      assert %{^ref => %Request{done?: false, status: 200}} =
               new_state.queue.requests
    end

    test "when the connection died, asks the dispatcher to reconnect" do
      socket = closed_socket()

      assert {:noreply, new_state} =
               HTTP.handle_stream(
                 {:error, socket, %Mint.TransportError{reason: :closed}, []},
                 state(open_socket()),
                 fn _request -> :ok end
               )

      assert new_state.socket == socket
      assert_received {:closed, ^socket}
    end

    test "when the server sent GOAWAY, asks the dispatcher to reconnect" do
      socket = draining_socket()

      error = %Mint.HTTPError{
        module: Mint.HTTP2,
        reason: {:server_closed_connection, :no_error, ""}
      }

      assert {:noreply, new_state} =
               HTTP.handle_stream(
                 {:error, socket, error, []},
                 state(open_socket()),
                 fn _request -> :ok end
               )

      assert new_state.socket == socket
      assert_received {:closed, ^socket}
    end

    test "when the stream errored but the connection survived, keeps using it" do
      socket = open_socket()

      error = %Mint.HTTPError{
        module: Mint.HTTP2,
        reason: {:exceeds_window_size, :request, 0}
      }

      assert {:noreply, new_state} =
               HTTP.handle_stream(
                 {:error, socket, error, []},
                 state(socket),
                 fn _request -> :ok end
               )

      assert new_state.socket == socket
      refute_received {:closed, _socket}
    end
  end
end
