import serviceAccount from "./service-account.json";
import * as admin from "firebase-admin";
import * as f from "firebase/firestore";
import { RateLimiterMemory } from "rate-limiter-flexible";
import cluster from "cluster";
import { cpus } from "os";
// import FirestoreDB from "./db";
import { RealtimeDB } from "./RealtimeDB/RealtimeDB";

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount as admin.ServiceAccount),
});

// @ts-ignore
const limiter = new RateLimiterMemory({
  points: 5, // 5 points
  duration: 1, // per second
});

const numCPUs = cpus().length;

console.log(`Number of CPUs: ${numCPUs}`);

if (cluster.isPrimary) {
  console.log(`Primary ${process.pid} is running`);

  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on("exit", (worker) => {
    console.log(`Worker ${worker.process.pid} died`);
    // cluster.fork(); // Restart worker
  });
} else {
  // Worker processes
  console.log(`Worker ${process.pid} started`);
  runFirestoreQueries()
    .then(() => {
      console.log(`Worker ${process.pid} finished`);
      process.exit(0);
    })
    .catch((err) => {
      console.error(`Worker ${process.pid} error:`, err);
      process.exit(1);
    });
}

async function runFirestoreQueries() {
  const realtimeDb = new RealtimeDB();

  const firestore = realtimeDb.getNativeFirestore();
  const collections = await firestore.listCollections();
  for (const collection of collections) {
    console.log("Collection ID:", collection.path);
  }
  const user = await realtimeDb.collection("users").doc("user1").get();
  // console.log("Fetched User:", user);

  if (!user) {
    await realtimeDb.collection("users").doc("user1").create({
      name: "John Doe",
      age: 30,
      email: "d@e.com",
      status: 0,
    });
  } else {
    const data = user.data();
    console.log("User already exists:", data);
    await realtimeDb.collection("users").doc("user1").set({ status: 0 });
  }

  try {
    await realtimeDb
      .collection("users")
      .doc("user1")
      .update({ age: 31, status: 0 });
  } catch (e) {
    if (isFireStoreError(e)) {
      console.error(e);
    } else {
      throw e;
    }
  }

  await Promise.all(
    Array.from({ length: 10 }, (_, i) => i + 1).map((i) =>
      realtimeDb.runTransaction(async (transaction) => {
        console.log(`[${process.pid}] Running transaction ${i}`);
        const docRef = realtimeDb.collection("users").doc("user1");
        const doc = await transaction.get(docRef);
        const data = doc.data();
        if (data && data.status < i) {
          console.log(
            `[${process.pid}] Updating status from ${data.status} to ${i}`
          );
          transaction.update(docRef, { status: i });
        } else {
          console.log(
            `[${process.pid}] Status is already ${data?.status}, aborting`
          );
        }
      })
    )
  );
}

function isFireStoreError(e: any): e is f.FirestoreError {
  return typeof e === "object" && typeof e.code === "number";
}
