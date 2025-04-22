import { Transaction } from "firebase-admin/firestore";
import { RealtimeDocumentReference } from "./RealtimeDocument";

export class RealtimeTransaction<T = any> {
  constructor(
    private readonly transaction: Transaction,
    private readonly updateFn: (t: RealtimeTransaction) => Promise<T>
  ) {}

  public execute(): Promise<T> {
    return this.updateFn(this);
  }

  public get(docRef: RealtimeDocumentReference) {
    return this.transaction.get(docRef.ref);
  }

  public update(docRef: RealtimeDocumentReference, data: Partial<T>) {
    return this.transaction.update(docRef.ref, data);
  }

  public delete(docRef: RealtimeDocumentReference) {
    return this.transaction.delete(docRef.ref);
  }

  public get ref(): Transaction {
    return this.transaction;
  }
}
