import serviceAccount from "./service-account.json";
import * as admin from "firebase-admin";
import * as f from "firebase/firestore";
import cluster from "cluster";
import { cpus } from "os";
// import FirestoreDB from "./db";
import { RealtimeDB } from "./RealtimeDB/RealtimeDB";

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount as admin.ServiceAccount),
});

const CLUSTERED_MODE = false;

export enum TestCase {
  StatusUpdate = "StatusUpdate",
  LatestTimestamp = "LatestTimestamp",
}

const numCPUs = cpus().length;

console.log(`Number of CPUs: ${numCPUs}`);

if (CLUSTERED_MODE) {
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
    bootstrap()
      .then(() => {
        console.log(`Worker ${process.pid} finished`);
        process.exit(0);
      })
      .catch((err) => {
        console.error(`Worker ${process.pid} error:`, err);
        process.exit(1);
      });
  }
}

class Main {
  private readonly realtimeDb: RealtimeDB = new RealtimeDB();

  async run(testCases: TestCase[] = []) {
    await this.seed();

    console.log("Seeding completed.");
    for (const testCase of testCases) {
      await this.runTestCaseScenario(testCase);
    }

    console.log("All test cases completed.");
  }

  async seed() {
    const user = await this.realtimeDb.collection("users").doc("user1").get();
    // console.log("Fetched User:", user);

    if (!user) {
      await this.realtimeDb.collection("users").doc("user1").create({
        name: "John Doe",
        age: 30,
        email: "d@e.com",
        status: 0,
      });
    } else {
      const data = user.data();
      console.log("User already exists:", data);
      await this.realtimeDb.collection("users").doc("user1").set({ status: 0 });
    }

    try {
      await this.realtimeDb
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
  }

  private async runTestCaseScenario(testCase: TestCase) {
    switch (testCase) {
      case TestCase.StatusUpdate:
        return this.runStatusUpdateTest();
      case TestCase.LatestTimestamp:
        return this.runLatestTimestampTest();
      default:
        throw new Error("Unknown test case");
    }
  }

  private async runStatusUpdateTest() {
    await Promise.all(
      Array.from({ length: 10 }, (_, i) => i + 1).map((i) =>
        this.realtimeDb.runTransaction(async (transaction) => {
          console.log(`[${process.pid}] Running transaction ${i}`);
          const docRef = this.realtimeDb.collection("users").doc("user1");
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

  private async runLatestTimestampTest() {
    this.realtimeDb.runTransaction(async (transaction) => {
      const docRef = this.realtimeDb.collection("users").doc("user1");
      const doc = await transaction.get(docRef);
      const timestamp = Date.now();
      const serverNow =
        admin.firestore.Timestamp.fromMillis(timestamp).toMillis();
      console.log(
        `[${
          process.pid
        }] Latest timestamp: ${doc.updateTime?.toMillis()}, current timestamp: ${serverNow}`
      );
    });
  }
}

async function bootstrap() {
  await new Main().run([TestCase.LatestTimestamp]);
}

bootstrap();

function isFireStoreError(e: any): e is f.FirestoreError {
  return typeof e === "object" && typeof e.code === "number";
}
