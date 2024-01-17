import "dotenv/config";
import { createCloudClient } from "./reader";
import { runTest } from "./seekTimestampTest";
import { countRecords, loadData } from "./util";

const topic = "persistent://safariai/edge/ryan-seekTimestap-test";
async function run() {
  const cloudClient = createCloudClient();
  const sleepMsBetweenRecords = 10;
  console.log("counting records");
  const recordCount = await countRecords(cloudClient, topic);
  console.log(`record count: ${recordCount}`);

  if (recordCount < 1500) {
    console.log("loading data");
    //TODO, use more effecient ways to load these records if we want to do a test on a larger
    //amount of local data
    await loadData(cloudClient, topic, 1500, sleepMsBetweenRecords);
  }

  await runTest({
    client: cloudClient,
    topic: topic,
    marginOfError: 10,
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
