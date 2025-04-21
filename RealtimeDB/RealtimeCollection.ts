import {
  CollectionReference,
  DocumentReference,
  Query,
  QuerySnapshot,
  DocumentData,
} from "firebase-admin/firestore";
import { RealtimeDocumentReference } from "./RealtimeDocument";

export class RealtimeCollectionReference<
  T extends DocumentData = DocumentData
> {
  constructor(private readonly collectionRef: CollectionReference<T>) {}

  public doc(documentPath?: string) {
    const docRef = documentPath
      ? this.collectionRef.doc(documentPath)
      : this.collectionRef.doc();
    return new RealtimeDocumentReference<T>(docRef);
  }

  public get() {
    console.log(`[RealtimeDB] Reading collection: ${this.collectionRef.path}`);
    return this.collectionRef.get();
  }

  public add(data: T): Promise<DocumentReference<T>> {
    console.log(
      `[RealtimeDB] Adding document to collection: ${this.collectionRef.path}`
    );
    return this.collectionRef.add(data);
  }

  public query(
    queryFn: (query: Query<T>) => Query<T>
  ): Promise<QuerySnapshot<T>> {
    console.log(`[RealtimeDB] Querying collection: ${this.collectionRef.path}`);
    const query = queryFn(this.collectionRef);
    return query.get();
  }

  get ref(): CollectionReference<T> {
    return this.collectionRef;
  }
}
