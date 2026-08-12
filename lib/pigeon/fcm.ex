defmodule Pigeon.FCM do
  @moduledoc """
  `Pigeon.Adapter` for Firebase Cloud Messaging (FCM) push notifications.

  ## Getting Started

  ### Create a dispatcher.

    ```
    # lib/your_app/fcm.ex

    defmodule YourApp.FCM do
      use Pigeon.Dispatcher, otp_app: :your_app
    end
    ```

  ### Install and configure Goth.

  Install and configure [`goth`](https://hexdocs.pm/goth/1.4.3/readme.html#installation)
  if you haven't already. `Pigeon.FCM` requires it for token authentication.

  ### Configure your dispatcher.

  Configure your `FCM` dispatcher and start it on application boot.

  ```
  # config.exs

  config :your_app, YourApp.FCM,
    adapter: Pigeon.FCM,
    auth: YourApp.Goth, # Your Goth worker configured in the previous step.
    project_id: "example-project-123"
  ```

  Add it to your supervision tree.

  ```
  defmodule YourApp.Application do
    @moduledoc false

    use Application

    @doc false
    def start(_type, _args) do
      children = [
        {Goth, name: YourApp.Goth},
        YourApp.FCM
      ]
      opts = [strategy: :one_for_one, name: YourApp.Supervisor]
      Supervisor.start_link(children, opts)
    end
  end
  ```

  If preferred, you can include your configuration directly.

  ```
  defmodule YourApp.Application do
    @moduledoc false

    use Application

    @doc false
    def start(_type, _args) do
      children = [
        {Goth, name: YourApp.Goth},
        {YourApp.FCM, fcm_opts()}
      ]
      opts = [strategy: :one_for_one, name: YourApp.Supervisor]
      Supervisor.start_link(children, opts)
    end

    defp fcm_opts do
      [
        adapter: Pigeon.FCM,
        auth: YourApp.Goth,
        project_id: "example-project-123"
      ]
    end
  end
  ```

  ### Create a notification.

  ```
  n = Pigeon.FCM.Notification.new({:token, "reg ID"}, %{"body" => "test message"})
  ```

  ### Send the notification.

  On successful response, `:name` will be set to the name returned from the FCM
  API and `:response` will be `:success`. If there was an error, `:error` will
  contain a JSON map of the response and `:response` will be an atomized version
  of the error type.

  ```
  YourApp.FCM.push(n)
  ```

  ## Customizing Goth

  You can use any of the configuration options (e.g. `:source`) for Goth. Check out the
  documentation of [`Goth.start_link/1`](https://hexdocs.pm/goth/Goth.html#start_link/1)
  for more details.
  """

  @behaviour Pigeon.Adapter

  import Pigeon.Tasks, only: [process_on_response: 1]

  alias Pigeon.Configurable
  alias Pigeon.FCM.{Config, Error}
  alias Pigeon.HTTP.{Request, RequestQueue}

  require Logger

  @max_retries 3

  defstruct config: nil,
            queue: RequestQueue.new(),
            retries: @max_retries,
            socket: nil

  @impl Pigeon.Adapter
  def init(opts) do
    config = Config.new(opts)

    Configurable.validate!(config)

    state = %__MODULE__{config: config}

    case connect_socket(config) do
      {:ok, socket} ->
        Configurable.schedule_ping(config)
        {:ok, %{state | socket: socket}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl Pigeon.Adapter
  def handle_push(notification, %{socket: socket} = state) do
    maybe_send(notification, state, Mint.HTTP.open?(socket, :write))
  end

  @impl Pigeon.Adapter
  def handle_info(:ping, %{socket: socket} = state) do
    maybe_ping(state, Mint.HTTP.open?(socket, :write))
  end

  def handle_info({:closed, _socket}, state) do
    reconnect(state)
  end

  def handle_info(msg, state) do
    Pigeon.HTTP.handle_info(msg, state, &handle_response/1)
  end

  @spec handle_response(Request.t()) :: :ok
  def handle_response(%{body: body, notification: notif}) do
    body
    |> Pigeon.json_library().decode!()
    |> case do
      %{"name" => name} ->
        notif
        |> Map.put(:name, name)
        |> Map.put(:response, :success)
        |> process_on_response()

      %{"error" => error} ->
        notif
        |> Map.put(:error, error)
        |> Map.put(:response, Error.parse(error))
        |> process_on_response()
    end
  end

  defp maybe_send(notification, state, true) do
    case send_request(notification, state) do
      {:ok, state} -> {:noreply, state}
      {:error, state, _reason} -> reconnect_and_send(notification, state)
    end
  end

  defp maybe_send(notification, state, false) do
    reconnect_and_send(notification, state)
  end

  # Retries the push exactly once, on a freshly connected socket. A push that
  # fails again is reported rather than retried, so an FCM outage degrades
  # delivery instead of spinning here.
  defp reconnect_and_send(notification, %{config: config} = state) do
    case connect_socket(config) do
      {:ok, socket} ->
        Configurable.schedule_ping(config)
        send_reconnected(notification, %{state | socket: socket})

      {:error, reason} ->
        {:noreply, report_unavailable(notification, state, reason)}
    end
  end

  defp send_reconnected(notification, state) do
    case send_request(notification, state) do
      {:ok, state} ->
        {:noreply, state}

      {:error, state, reason} ->
        {:noreply, report_unavailable(notification, state, reason)}
    end
  end

  defp send_request(notification, state) do
    %{config: config, queue: queue, socket: socket} = state
    headers = Configurable.push_headers(config, notification, [])
    payload = Configurable.push_payload(config, notification, [])
    method = "POST"
    path = "/v1/projects/#{config.project_id}/messages:send"

    case Mint.HTTP.request(socket, method, path, headers, payload) do
      {:ok, socket, ref} ->
        queue = RequestQueue.add(queue, ref, notification)
        {:ok, %{state | socket: socket, queue: queue}}

      {:error, socket, reason} ->
        {:error, %{state | socket: socket}, reason}
    end
  end

  # A notification that never reached the wire still has to reach the caller's
  # on_response, otherwise its delivery status is silently lost.
  defp report_unavailable(notification, state, reason) do
    Logger.error("FCM push could not be sent: #{inspect(reason)}")

    notification
    |> Map.put(:error, reason)
    |> Map.put(:response, :unavailable)
    |> process_on_response()

    state
  end

  defp maybe_ping(%{config: config, socket: socket} = state, true) do
    case Mint.HTTP2.ping(socket) do
      {:ok, socket, _ref} ->
        Configurable.schedule_ping(config)
        {:noreply, %{state | socket: socket}}

      {:error, socket, _reason} ->
        reconnect(%{state | socket: socket})
    end
  end

  defp maybe_ping(state, false), do: reconnect(state)

  defp reconnect(%{config: config} = state) do
    case connect_socket(config) do
      {:ok, socket} ->
        Configurable.schedule_ping(config)
        {:noreply, %{state | socket: socket}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @spec connect_socket(Config.t()) :: {:ok, Mint.HTTP2.t()} | {:error, term()}
  defp connect_socket(config), do: connect_socket(config, @max_retries)

  defp connect_socket(config, tries) do
    case Configurable.connect(config) do
      {:ok, socket} ->
        {:ok, socket}

      {:error, reason} ->
        if tries > 0 do
          connect_socket(config, tries - 1)
        else
          {:error, reason}
        end
    end
  end
end
