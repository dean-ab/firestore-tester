import * as admin from "firebase-admin";
import {
  Firestore,
  DocumentReference,
  Query,
  WriteResult,
  DocumentSnapshot,
  QuerySnapshot,
} from "firebase-admin/firestore";
import { RealtimeCollectionReference } from "./RealtimeCollection";
import { RealtimeDocumentReference } from "./RealtimeDocument";
import { RealtimeTransaction } from "./RealtimeTransaction";

export class RealtimeDB {
  private firestore: Firestore;

  public constructor() {
    if (!admin.apps.length) {
      admin.initializeApp({
        credential: admin.credential.applicationDefault(),
      });
    }
    this.firestore = admin.firestore();
  }

  /**
   * Get a collection reference
   */
  public collection(collectionPath: string): RealtimeCollectionReference {
    return new RealtimeCollectionReference(
      this.firestore.collection(collectionPath)
    );
  }

  /**
   * Get a document reference
   */
  public doc(documentPath: string): RealtimeDocumentReference {
    return new RealtimeDocumentReference(this.firestore.doc(documentPath));
  }

  /**
   * Create a document with auto-generated ID
   */
  public add(collectionPath: string, data: any): Promise<DocumentReference> {
    const collectionRef = this.collection(collectionPath);
    return collectionRef.add(data);
  }

  /**
   * Set a document's data
   */
  public set(
    documentPath: string,
    data: any,
    options?: any
  ): Promise<WriteResult> {
    const docRef = this.doc(documentPath);
    return docRef.set(data, options);
  }

  /**
   * Update a document's data
   */
  public update(documentPath: string, data: any): Promise<WriteResult> {
    const docRef = this.doc(documentPath);
    return docRef.update(data);
  }

  /**
   * Delete a document
   */
  public delete(documentPath: string): Promise<WriteResult> {
    const docRef = this.doc(documentPath);
    return docRef.delete();
  }

  /**
   * Get a document
   */
  public get(documentPath: string): Promise<DocumentSnapshot> {
    const docRef = this.doc(documentPath);
    return docRef.get();
  }

  /**
   * Query documents in a collection
   */
  public query(
    collectionPath: string,
    queryFn: (query: Query) => Query
  ): Promise<QuerySnapshot> {
    const collection = this.collection(collectionPath);
    return collection.query(queryFn);
  }

  /**
   * Access the native Firestore instance directly if needed
   */
  public getNativeFirestore(): Firestore {
    return this.firestore;
  }

  /**
   * Batch operations
   */
  public batch() {
    return this.firestore.batch();
  }

  /**
   * Run a transaction
   */
  public runTransaction<T>(
    updateFunction: (transaction: RealtimeTransaction) => Promise<T>
  ): Promise<T> {
    return this.firestore.runTransaction((t) =>
      new RealtimeTransaction(t, updateFunction).execute()
    );
  }
}
