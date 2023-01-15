const { MongoClient } = require("mongodb");
var Chance = require("chance");
const { faker } = require("@faker-js/faker");

// Instantiate Chance so it can be used
var chance = new Chance();

// Use Chance here.
var my_random_string = chance.string();

// Replace the uri string with your connection string.
const uri = "mongodb://127.0.0.1:28050?retryWrites=true&w=majority";

const client = new MongoClient(uri);

const insertSongs = async () => {
  try {
    const database = client.db("spotifydb");
    const songs = database.collection("songs");

    let tmpArr = [];

    for (let i = 0; i < 1000000; i++) {
      let tmp = {
        name: faker.music.songName(),
        artist: chance.name(),
        feature: [chance.name(), chance.name()],
        genre: faker.music.genre(),
        data: chance.date({ string: true, american: false }),
        rating: chance.integer({ min: 0, max: 10 }),
      };

      tmpArr.push(tmp);
    }

    await songs.insertMany(tmpArr);
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
};

const insertUsers = async () => {
  try {
    const database = client.db("spotifydb");
    // user
    const users = database.collection("users");
    // get random record from collection movie

    let tmpUsers = [];
    for (let i = 0; i < 1000000; i++) {
      let tmp = {
        name: chance.name(),
        listened_songs: [],
        favorite_genres: [
          faker.music.genre(),
          faker.music.genre(),
          faker.music.genre(),
        ],
        subscription_month: chance.date({ string: true, american: false }),
      };
      tmpUsers.push(tmp);
    }
    await users.insertMany(tmpUsers);
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
};

const testMultiTransactionCluster = async () => {
  // For a replica set, include the replica set name and a seedlist of the members in the URI string; e.g.
  // const uri = 'mongodb://mongodb0.example.com:27017,mongodb1.example.com:27017/?replicaSet=myRepl'
  // For a sharded cluster, connect to the mongos instances; e.g.
  // const uri = 'mongodb://mongos0.example.com:27017,mongos1.example.com:27017/'

  const client = new MongoClient(uri);
  await client.connect();

  // Step 1: Start a Client Session
  const session = client.startSession();

  // Step 2: Optional. Define options to use for the transaction
  const transactionOptions = {
    readPreference: "primary",
    readConcern: { level: "local" },
    writeConcern: { w: "majority" },
  };

  // Step 3: Use withTransaction to start a transaction, execute the callback, and commit (or abort on error)
  // Note: The callback for withTransaction MUST be async and/or return a Promise.
  try {
    await session.withTransaction(async () => {
      const coll1 = client.db("spotifydb").collection("songs");
      const coll2 = client.db("spotifydb").collection("users");

      // Important:: You must pass the session to the operations

      await coll2.insertOne({ name: "user-test-multi-trans" });
      await coll1.updateOne(
        { name: "session-1010" },
        { $set: { name: "multi-trans-test-songs-1000" } },
        { session }
      );

      console.log("Timeout start");
      await new Promise((resolve) => setTimeout(resolve, 60000));
      console.log("Timeout ends");
    }, transactionOptions);
  } finally {
    await session.endSession();
    await client.close();
  }
};

async function run() {
  const start = Date.now();

  try {
    // await insertSongs();
    // await insertUsers();
    await testMultiTransactionCluster();
  } catch (err) {
    console.log(err);
  }

  const end = Date.now();
  console.log(`Execution time: ${end - start} ms`);
}
run().catch(console.dir);
