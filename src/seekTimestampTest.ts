import "dotenv/config";
import Pulsar from "pulsar-client";
import { readData } from "./reader";

type runTestProps = {
  client: Pulsar.Client;
  topic: string;
  marginOfError?: number;
};

export async function runTest(props: runTestProps) {
  const { client, topic } = props;
  const marginOfError = props.marginOfError ?? 1000;

  const firstResult = await readData({
    client,
    topic,
    maxRecordsToRead: 1,
  });
  if (!firstResult?.lastMessage) {
    throw new Error("No records found");
  }
  // This is the timestamp of the first record in the topic
  const firstRecordTimestamp = firstResult?.lastMessage.getEventTimestamp();

  const scanResult = await readData({
    client,
    topic,
    maxRecordsToRead: 1000,
  });
  if (!scanResult?.lastMessage) {
    throw new Error("last record not found");
  }
  if (!(scanResult.actualRecordsRead === 1000)) {
    throw new Error(
      "Expected to be able to read 1000 records, only read " +
        scanResult.actualRecordsRead
    );
  }

  const scanRecordTimestamp = scanResult?.lastMessage.getEventTimestamp();

  const seekTimestamp = scanRecordTimestamp;
  const seekResult = await readData({
    client,
    topic,
    seekTimestamp: scanRecordTimestamp,
    maxRecordsToRead: 1,
  });

  if (!seekResult?.lastMessage) {
    throw new Error("seek result not found");
  }

  const seekResultTimestamp = seekResult?.lastMessage.getEventTimestamp();

  console.log("timestamp of first record          : ", firstRecordTimestamp);
  console.log("timestamp of 1000th scanned record : ", scanRecordTimestamp);
  console.log("timestamp of seekTimestamp record  : ", seekResultTimestamp);

  const seekDelta = seekTimestamp - seekResultTimestamp;
  console.log(
    "diff between seek and seek result timestamps (expected to be 0 or close to it)",
    seekDelta
  );

  if (Math.abs(seekDelta) > marginOfError) {
    throw new Error(
      "Seek result timestamp is too far from seek timestamp. Expected to be 0 or close to it."
    );
  }
}
