import {
  DocumentReference,
  WriteResult,
  DocumentSnapshot,
  DocumentData,
  SetOptions,
} from "firebase-admin/firestore";
import { RealtimeCollectionReference } from "./RealtimeCollection";

export class RealtimeDocumentReference<
  T extends DocumentData = FirebaseFirestore.DocumentData
> {
  constructor(private readonly docRef: DocumentReference<T>) {}

  public get(): Promise<DocumentSnapshot<T>> {
    console.log(`[RealtimeDB] Reading document: ${this.docRef.path}`);
    return this.docRef.get();
  }

  public create(data: T): Promise<WriteResult> {
    console.log(`[RealtimeDB] Creating document: ${this.docRef.path}`);
    return this.docRef.create(data);
  }

  public set(data: T, options?: SetOptions): Promise<WriteResult> {
    console.log(`[RealtimeDB] Setting document: ${this.docRef.path}`);
    if (options) {
      return this.docRef.set(data, options);
    }
    return this.docRef.set(data);
  }

  public update(data: Partial<T>): Promise<WriteResult> {
    console.log(`[RealtimeDB] Updating document: ${this.docRef.path}`);
    return this.docRef.update(data);
  }

  public delete(): Promise<WriteResult> {
    console.log(`[RealtimeDB] Deleting document: ${this.docRef.path}`);
    return this.docRef.delete();
  }

  public collection(collectionPath: string): RealtimeCollectionReference {
    return new RealtimeCollectionReference(
      this.docRef.collection(collectionPath)
    );
  }

  get ref(): DocumentReference<T> {
    return this.docRef;
  }
}
