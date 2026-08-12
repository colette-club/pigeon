defmodule Pigeon.HTTP do
  @moduledoc false

  alias Pigeon.HTTP.RequestQueue
  require Logger

  @type handler :: (Pigeon.HTTP.Request.t() -> any())

  @spec handle_info(term(), term(), handler()) :: {:noreply, term()}
  def handle_info(message, %{socket: socket} = state, handler) do
    socket
    |> Mint.HTTP.stream(message)
    |> handle_stream(state, handler)
  end

  @doc """
  Applies the result of `Mint.HTTP.stream/2` to the dispatcher state.

  Public so each branch -- in particular the dead-connection one, which is
  awkward to provoke through a real socket -- can be exercised directly.
  """
  @spec handle_stream(term(), term(), handler()) :: {:noreply, term()}
  def handle_stream(:unknown, state, _handler), do: {:noreply, state}

  def handle_stream({:ok, socket, responses}, %{queue: queue} = state, handler) do
    {done, queue} =
      responses
      |> RequestQueue.process(queue)
      |> RequestQueue.pop_done()

    for {_ref, request} <- done do
      if request.notification, do: handler.(request)
    end

    {:noreply, %{state | queue: queue, socket: socket}}
  end

  def handle_stream({:error, socket, error, _responses}, state, _handler) do
    error |> inspect(pretty: true) |> Logger.error()

    state = %{state | socket: socket}

    maybe_notify_closed(state, Mint.HTTP.open?(socket, :write))
  end

  # Mint keeps handing back the connection struct after a stream error, dead or
  # not, so the adapter would otherwise keep a socket it can no longer write to
  # and raise on the next push. Asking Mint whether the connection is still
  # writable covers every way it can die -- transport close, GOAWAY, protocol
  # error -- without enumerating reasons.
  defp maybe_notify_closed(state, true), do: {:noreply, state}

  defp maybe_notify_closed(%{socket: socket} = state, false) do
    send(self(), {:closed, socket})
    {:noreply, state}
  end
end
