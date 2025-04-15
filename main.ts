import serviceAccount from "./service-account.json";
import * as admin from "firebase-admin";
import * as f from "firebase/firestore";
import { RateLimiterMemory } from "rate-limiter-flexible";
import cluster from "cluster";
import { cpus } from "os";
import FirestoreDB from "./db";

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
  const firestore = new FirestoreDB();

  const user = await firestore.getUser("user1");
  console.log("Fetched User:", user);

  if (!user) {
    await firestore.addUser("user1", {
      name: "John Doe",
      age: 30,
      email: "d@e.com",
      status: 0,
    });
  }

  try {
    await firestore.updateUser("user1", { age: 31 });
  } catch (e) {
    if (isFireStoreError(e)) {
      console.log("name", e.name);
      console.log("code", getFirestoreClientErrorCode(e));
      console.log("message", e.message);
      console.log("cause", e.cause);
      console.log("customData", e.customData);
      console.log("stack", e.stack);
    }
  }

  await Promise.all(
    Array.from({ length: 10 }, (_, i) => i + 1).map((i) =>
      firestore.updateStatus("user1", i)
    )
  );

  await firestore.deleteUser("user1");
}

function isFireStoreError(e: any): e is f.FirestoreError {
  return typeof e === "object" && typeof e.code === "number";
}

enum GrpcStatus {
  OK = 0,
  CANCELLED = 1,
  UNKNOWN = 2,
  INVALID_ARGUMENT = 3,
  DEADLINE_EXCEEDED = 4,
  NOT_FOUND = 5,
  ALREADY_EXISTS = 6,
  PERMISSION_DENIED = 7,
  RESOURCE_EXHAUSTED = 8,
  FAILED_PRECONDITION = 9,
  ABORTED = 10,
  OUT_OF_RANGE = 11,
  UNIMPLEMENTED = 12,
  INTERNAL = 13,
  UNAVAILABLE = 14,
  DATA_LOSS = 15,
  UNAUTHENTICATED = 16,
}

function getFirestoreClientErrorCode(error: any): any {
  console.log("error.code", error.message);
  if (typeof error !== "object" || typeof error.code !== "number") {
    return "UNKNOWN";
  }
  const codeIdx = Object.values(GrpcStatus).indexOf(error.code);
  return (Object.keys(GrpcStatus)[codeIdx] as any) || "UNKNOWN";
}
