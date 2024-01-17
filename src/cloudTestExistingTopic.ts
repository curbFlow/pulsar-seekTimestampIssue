import "dotenv/config";
import { createCloudClient } from "./reader";
import { runTest } from "./seekTimestampTest";

const cloudTopic = process.env.PULSAR_TOPIC!;

async function run() {
  const cloudClient = createCloudClient();
  await runTest({
    client: cloudClient,
    topic: cloudTopic,
    marginOfError: 1000,
  });
}

run()
  .then(() => {
    console.log("Done!");
    process.exit(0);
  })
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
