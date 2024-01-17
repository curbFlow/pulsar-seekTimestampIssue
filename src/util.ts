import { get } from "http";
import Pulsar from "pulsar-client";
import { getPulsarReader } from "./reader";

export async function createTestConsumer(client: Pulsar.Client, topic: string) {
  const consumer = await client.subscribe({
    topic,
    subscription: "test-subscription",
    consumerName: "test-consumer",
    ackTimeoutMs: 10000,
  });

  const msg = await consumer.receive();
  await consumer.acknowledge(msg);
  // Don't read more messages... just want to add a consumer so local pulsar doesn't auto-clean read messages
}

export async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function loadData(
  client: Pulsar.Client,
  topic: string,
  numRecords: number,
  sleepMs: number = 100
) {
  const producer = await client.createProducer({
    topic,
    sendTimeoutMs: 30000,
  });

  for (let i = 0; i < numRecords; i++) {
    console.log(`Sending message ${i}`);
    await sleep(sleepMs);
    const now = Date.now();
    await producer.send({
      data: Buffer.from(`hello at ${now} (${new Date(now).toUTCString()})`),
      eventTimestamp: now,
    });
  }
  await producer.close();
}

export async function countRecords(
  client: Pulsar.Client,
  topic: string
): Promise<number> {
  try {
    const reader = await getPulsarReader(client, "count-records", topic);
    let count = 0;
    while (reader.hasNext()) {
      await reader.readNext();
      count++;
    }
    return count;
  } catch (e) {
    return 0;
  }
}
