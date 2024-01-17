# Pulsar Cloud seekTimestamp() issue

This code reproduces an issue with the seekTimestamp() method of the Pulsar Cloud NodeJS client.

Interestingly, this bug only seems to affect the Pulsar Cloud service, and not the self-hosted Pulsar service. Use the included docker-compose file to spin up a self-hosted Pulsar service for testing (this compose file was just copied directly from the pulsar quickstart docs at the time)

Then install the nodejs dependencies with `yarn install`

If you then run the command `yarn test:local`, this will:
1. Insert 1500 messages into a test topic on the local cluster
2. Create a reader and read the first message in the topic, noting the timestamp of that message
3. Create a new reader and seek 1000 records forward through the topic (starting at the beginning) and not the timestamp of the last record we end up on
4. Create a new reader, call the `seekTimestamp()` method with the timestamp from step 3, and then read the first record from the topic
5. Checks to make sure that the message read in step 4 is the same (or at least very close to) the message timestamp from step 3


When this is run on my local machine (I developed & tested this code using node 20.3.0 on a MacOS M1) against the local pulsar cluster, the test passes and the message read after the seekTimestamp() matches the timestamp passed into the method as expected.

When I run from my local machine against our streamnative cloud infrastructure, the tests pass

To run this test on streamnative:
1. Rename `.env.sample` to `.env` and fill in the values for your streamnative account
2. Run `yarn test:cloud`

When I run this test against the streamnative cloud service, step 5 of the test script fails. Instead of returning a record at or near the seekTimestamp parameter as expected, it is instead still returns the first record in the topic. This is preventing several of our production systems from operating correctly.