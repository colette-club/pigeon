ExUnit.start(capture_log: true)

# The live dispatchers below talk to APNS/FCM/ADM and need real credentials.
# When those are absent (a plain `mix test` on a contributor's machine, or CI
# for a fork without secrets), start only the offline workers and exclude the
# tests that require a network round-trip, so the unit suite still runs.
case System.fetch_env("FCM_SERVICE_ACCOUNT_JSON") do
  {:ok, json} ->
    workers = [
      {Goth,
       name: PigeonTest.Goth,
       source: {:service_account, Jason.decode!(json), []}},
      PigeonTest.ADM,
      PigeonTest.APNS,
      PigeonTest.APNS.JWT,
      PigeonTest.FCM,
      PigeonTest.Sandbox
    ]

    Supervisor.start_link(workers, strategy: :one_for_one)

  :error ->
    ExUnit.configure(exclude: [:integration])

    # A stubbed Goth, so the adapters can build real push headers offline. The
    # refresh-token source signs nothing, so no key material is needed, and the
    # stub http_client keeps the exchange off the network.
    token_response = %{
      "access_token" => "stub-access-token",
      "expires_in" => 3600,
      "token_type" => "Bearer"
    }

    stub_goth = [
      name: PigeonTest.Goth,
      source:
        {:refresh_token,
         %{
           "client_id" => "stub-client-id",
           "client_secret" => "stub-client-secret",
           "refresh_token" => "stub-refresh-token"
         }, []},
      http_client: fn _opts ->
        {:ok, %{status: 200, headers: [], body: Jason.encode!(token_response)}}
      end
    ]

    Supervisor.start_link([{Goth, stub_goth}, PigeonTest.Sandbox],
      strategy: :one_for_one
    )
end
