defmodule MyApp.Broadway do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: producer_module(),
        concurrency: 1
      ],
      processors: [
        default: [
          concurrency: 3
        ]
      ]
    )
  end

  defp producer_module do
    {BroadwayKafka.Producer,
     [
       hosts: "localhost:9092",
       group_id: "foo_group_id2",
       topics: ["event.foo"]
     ]}
  end

  @impl true
  def handle_message(_, message, _) do
    call_me(message)
    message
  end

  defp call_me(message) do
    Process.sleep(100)
  end

  def create_message do
    spawn(fn ->
      key = fn -> :crypto.strong_rand_bytes(16) |> Base.encode16() end

      Enum.each(1..100_000, fn i ->
        :brod.produce_sync(:foo_producer, "event.foo", :hash, key.(), "body #{i}")
      end)
    end)
  end
end
