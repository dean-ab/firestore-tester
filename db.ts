import admin, { firestore } from "firebase-admin";

export interface User {
  id: string;
  name: string;
  email: string;
  age: number;
  status: number;
}

class FirestoreDB {
  public db: firestore.Firestore;
  public collection: firestore.CollectionReference<firestore.DocumentData>;
  constructor() {
    if (!admin.apps.length) {
      admin.initializeApp({
        credential: admin.credential.applicationDefault(),
      });
    }
    this.db = admin.firestore();
    this.collection = this.db.collection("users");
  }

  async addUser(id: string, data: Omit<User, "id">) {
    await this.collection.doc(id).create(data);
    console.log(`User ${id} added.`);
  }

  async getUser(id: string) {
    const doc = await this.collection.doc(id).get();
    if (!doc.exists) {
      console.log("No such user!");
      return null;
    }
    return doc.data();
  }

  async updateUser(id: string, data: Omit<Partial<User>, "id">) {
    await this.collection.doc(id).update(data);
    console.log(`User ${id} updated.`);
  }

  async deleteUser(id: string) {
    await this.collection.doc(id).delete();
    console.log(`User ${id} deleted.`);
  }

  async updateStatus(id: string, status: number) {
    await this.db.runTransaction(async (t) => {
      const doc = await t.get(this.collection.doc(id));
      if (!doc.exists) {
        throw new Error("Document does not exist!");
      }
      const data = doc.data();
      if (!data) {
        throw new Error("Document data is undefined!");
      }

      if (data.status < status) {
        t.update(this.collection.doc(id), { status });
      } else {
        console.log(
          `Status is not updated because it is not greater than the current status. Current: ${data.status}, New: ${status}`
        );
      }
    });
  }
}

export default FirestoreDB;
