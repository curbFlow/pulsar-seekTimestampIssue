import Pulsar from "pulsar-client";
import { writeFileSync } from "fs";
const serviceUrl = process.env.PULSAR_SERVICE_URL!;
const issuerUrl = process.env.PULSAR_ISSUER_URL!;
const audience = process.env.PULSAR_AUDIENCE!;
const privateKey = process.env.PULSAR_CLIENT_AUTH!;
const privateKeyFile = "./pulsar-creds.json";

if (privateKey) {
  console.log("Writing private key to file", privateKey);
  writeFileSync(privateKeyFile, privateKey, { flag: "w" });

  console.log(
    `Creating Cloud Pulsar Client with ${serviceUrl} with issuer ${issuerUrl}`
  );
  console.log(`Audience: ${audience}`);
  console.log(`Private Key: ${privateKeyFile}`);
}

export function createCloudClient() {
  return new Pulsar.Client({
    serviceUrl: serviceUrl,
    authentication: new Pulsar.AuthenticationOauth2({
      type: "client_credentials",
      issuer_url: issuerUrl,
      private_key: privateKeyFile,
      audience: audience,
    }),
    operationTimeoutSeconds: 15 * 60,
    tlsAllowInsecureConnection: false,
  });
}

export function createLocalClient() {
  return new Pulsar.Client({
    serviceUrl: "pulsar://localhost:6650",
    operationTimeoutSeconds: 30,
  });
}

export async function getPulsarReader(
  client: Pulsar.Client,
  name: string,
  topic: string
) {
  const readerConfig = {
    topic: topic,
    startMessageId: Pulsar.MessageId.earliest(),
    readerName: name,
    readCompacted: true,
  };
  console.log(
    `Creating reader with config ${JSON.stringify(readerConfig, null, 2)}`
  );
  const reader = await client.createReader(readerConfig);
  console.log("reader created");
  if (!reader.hasNext()) {
    throw new Error(`Topic empty or not found: ${topic}`);
  }
  return reader;
}

type readDataProps = {
  client: Pulsar.Client;
  topic: string;
  maxRecordsToRead?: number;
  seekTimestamp?: number;
  readerName?: string;
};

export async function readData(props: readDataProps) {
  const { topic, client, seekTimestamp } = props;
  const maxRecordsToRead = props.maxRecordsToRead ?? 1000;
  const readerName = props.readerName ?? "test-reader";

  console.log(`Reading from topic ${topic} starting at ${seekTimestamp}`);
  const reader = await getPulsarReader(client, readerName, topic);
  try {
    if (seekTimestamp) {
      console.log("seeking to timestamp: ", seekTimestamp);
      await reader.seekTimestamp(seekTimestamp);
    }

    let msg: Pulsar.Message | null = null;
    let recordsRead = 0;
    process.stdout.write("Reading pulsar records .");
    while (reader.hasNext() && recordsRead < maxRecordsToRead) {
      if (!reader.hasNext()) {
        console.log("No more messages to read");
        break;
      }

      msg = await reader.readNext();
      recordsRead++;
      process.stdout.write(".");
    }
    console.log(`\n Read ${recordsRead} records`);

    return {
      lastMessage: msg,
      actualRecordsRead: recordsRead,
    };
  } catch (e) {
    console.error(e);
  } finally {
    await reader.close();
  }
}
