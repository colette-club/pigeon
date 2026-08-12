defmodule Pigeon.ADM.ResultParserTest do
  use ExUnit.Case
  doctest Pigeon.ADM.ResultParser, import: true

  alias Pigeon.ADM.{Notification, ResultParser}

  test "parses known error reasons without crashing" do
    n = Notification.new("test")

    ResultParser.parse(n, %{"reason" => "InvalidRegistrationId"})
    ResultParser.parse(n, %{"reason" => "InvalidData"})
    ResultParser.parse(n, %{"reason" => "InvalidConsolidationKey"})
    ResultParser.parse(n, %{"reason" => "InvalidExpiration"})
    ResultParser.parse(n, %{"reason" => "InvalidChecksum"})
    ResultParser.parse(n, %{"reason" => "InvalidType"})
    ResultParser.parse(n, %{"reason" => "Unregistered"})
    ResultParser.parse(n, %{"reason" => "AccessTokenExpired"})
    ResultParser.parse(n, %{"reason" => "MessageTooLarge"})
    ResultParser.parse(n, %{"reason" => "MaxRateExceeded"})
  end

  test "handles unknown reasons" do
    n = Notification.new("test")
    ResultParser.parse(n, %{"reason" => "NotReal"})
  end
end
