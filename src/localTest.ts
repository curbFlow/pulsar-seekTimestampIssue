import "dotenv/config";
import { createLocalClient } from "./reader";
import { runTest } from "./seekTimestampTest";
import { countRecords, loadData } from "./util";

async function run() {
  const localClient = createLocalClient();
  const topic = "persistent://public/default/test-topic1";
  const sleepMsBetweenRecords = 10;
  console.log("counting records");
  const recordCount = await countRecords(localClient, topic);
  console.log(`record count: ${recordCount}`);

  if (recordCount < 2000) {
    console.log("loading data");
    //TODO, use more effecient ways to load these records if we want to do a test on a larger
    //amount of local data
    await loadData(localClient, topic, 2000, sleepMsBetweenRecords);
  }

  await runTest({
    client: localClient,
    topic,
    marginOfError: 2 * sleepMsBetweenRecords,
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
