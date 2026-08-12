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

    Supervisor.start_link([PigeonTest.Sandbox], strategy: :one_for_one)
end
