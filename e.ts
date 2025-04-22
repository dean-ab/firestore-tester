// realtime-db-refactored.ts
import * as admin from 'firebase-admin';
import { Firestore, CollectionReference, DocumentReference, Query, WriteResult, DocumentSnapshot, QuerySnapshot } from 'firebase-admin/firestore';
import { RateLimiterService } from './rate-limiter';
import { RateLimitedBatch, RateLimitedTransaction, DeferredOperationProcessor } from './deferred-transaction';

// Types for our deferred write operations
interface DeferredWrite {
  id: string;
  operation: 'add' | 'set' | 'update' | 'delete';
  path: string;
  data?: any;
  options?: any;
  timestamp: number;
  customerId: string;
}

// Interface for storage of deferred writes
interface DeferredWriteStorage {
  store(operation: DeferredWrite): Promise<void>;
  process(handler: (operation: DeferredWrite) => Promise<void>): Promise<void>;
}

export class RealtimeDB {
  private firestore: Firestore;
  private static instance: RealtimeDB;
  private currentCustomerId: string = 'default';
  
  // Optional components - can be null/undefined to bypass functionality
  private rateLimiter?: RateLimiterService;
  private deferredStorage?: DeferredWriteStorage;
  private operationProcessor: DeferredOperationProcessor;
  
  // For error handling
  private errorHandlers: ((error: Error, operation: DeferredWrite) => Promise<boolean>)[] = [];
  private dlqHandler?: (error: Error, operation: DeferredWrite) => Promise<void>;

  constructor(
    firestoreInstance?: Firestore,
    rateLimiter?: RateLimiterService,
    deferredStorage?: DeferredWriteStorage
  ) {
    this.firestore = firestoreInstance || admin.firestore();
    this.rateLimiter = rateLimiter;
    this.deferredStorage = deferredStorage;
    this.operationProcessor = new DeferredOperationProcessor(this.firestore);
  }

  /**
   * Get singleton instance of RealtimeDB
   */
  public static getInstance(
    firestoreInstance?: Firestore,
    rateLimiter?: RateLimiterService,
    deferredStorage?: DeferredWriteStorage
  ): RealtimeDB {
    if (!RealtimeDB.instance) {
      RealtimeDB.instance = new RealtimeDB(firestoreInstance, rateLimiter, deferredStorage);
    }
    return RealtimeDB.instance;
  }

  /**
   * Set the current customer ID context
   */
  public setCustomerId(customerId: string): void {
    this.currentCustomerId = customerId;
  }

  /**
   * Register error handler for deferred writes
   */
  public registerErrorHandler(handler: (error: Error, operation: DeferredWrite) => Promise<boolean>): void {
    this.errorHandlers.push(handler);
  }

  /**
   * Register DLQ handler for unrecoverable errors
   */
  public registerDLQHandler(handler: (error: Error, operation: DeferredWrite) => Promise<void>): void {
    this.dlqHandler = handler;
  }

  /**
   * Check if an operation should be rate limited
   */
  private async shouldRateLimit(points: number = 1): Promise<boolean> {
    if (!this.rateLimiter) {
      return false; // No rate limiter configured, don't rate limit
    }
    
    return await this.rateLimiter.isRateLimited(this.currentCustomerId, points);
  }

  /**
   * Defer a write operation
   */
  private async deferWrite(operation: DeferredWrite): Promise<void> {
    if (!this.deferredStorage) {
      throw new Error('Rate limit exceeded but no deferred storage configured');
    }
    
    await this.deferredStorage.store(operation);
    console.log(`Deferred write operation: ${operation.id}`);
  }

  /**
   * Handle errors for direct operations
   */
  private async handleError(error: Error, operation: DeferredWrite): Promise<void> {
    let recoverable = false;
    
    // Try registered error handlers
    for (const handler of this.errorHandlers) {
      try {
        recoverable = await handler(error, operation);
        if (recoverable) break;
      } catch (handlerError) {
        console.error('Error handler failed:', handlerError);
      }
    }
    
    // If not recoverable, send to DLQ
    if (!recoverable && this.dlqHandler) {
      await this.dlqHandler(error, operation);
    }
  }

  // =========== STANDARD DB OPERATIONS ===========

  /**
   * Get a collection reference (read operation - no rate limiting)
   */
  public collection(collectionPath: string): CollectionReference {
    return this.firestore.collection(collectionPath);
  }

  /**
   * Get a document reference (read operation - no rate limiting)
   */
  public doc(documentPath: string): DocumentReference {
    return this.firestore.doc(documentPath);
  }

  /**
   * Create a document with auto-generated ID (write operation - rate limited)
   */
  public async add(collectionPath: string, data: any): Promise<DocumentReference> {
    const customerId = this.currentCustomerId;
    
    // Check rate limit if enabled
    const shouldLimit = await this.shouldRateLimit();
    
    if (shouldLimit) {
      // Defer write operation
      const deferredOp: DeferredWrite = {
        id: `add-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
        operation: 'add',
        path: collectionPath, 
        data: data,
        timestamp: Date.now(),
        customerId: customerId
      };
      
      await this.deferWrite(deferredOp);
      
      // Return a placeholder reference
      return this.firestore.collection(collectionPath).doc(`pending-${deferredOp.id}`);
    }
    
    try {
      const collectionRef = this.collection(collectionPath);
      const result = await collectionRef.add(data);
      return result;
    } catch (error) {
      await this.handleError(error as Error, {
        operation: 'add',
        path: collectionPath,
        data: data,
        timestamp: Date.now(),
        customerId: customerId,
        id: `error-${Date.now()}`
      });
      throw error;
    }
  }

  /**
   * Set a document's data (write operation - rate limited)
   */
  public async set(documentPath: string, data: any, options?: any): Promise<WriteResult> {
    const customerId = this.currentCustomerId;
    
    // Check rate limit if enabled
    const shouldLimit = await this.shouldRateLimit();
    
    if (shouldLimit) {
      // Defer write operation
      const deferredOp: DeferredWrite = {
        id: `set-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
        operation: 'set',
        path: documentPath,
        data: data,
        options: options,
        timestamp: Date.now(),
        customerId: customerId
      };
      
      await this.deferWrite(deferredOp);
      
      // Return a placeholder WriteResult
      return { writeTime: admin.firestore.Timestamp.now() } as WriteResult;
    }
    
    try {
      const docRef = this.doc(documentPath);
      const result = await docRef.set(data, options);
      return result;
    } catch (error) {
      await this.handleError(error as Error, {
        operation: 'set',
        path: documentPath,
        data: data,
        options: options,
        timestamp: Date.now(),
        customerId: customerId,
        id: `error-${Date.now()}`
      });
      throw error;
    }
  }

  /**
   * Update a document's data (write operation - rate limited)
   */
  public async update(documentPath: string, data: any): Promise<WriteResult> {
    const customerId = this.currentCustomerId;
    
    // Check rate limit if enabled
    const shouldLimit = await this.shouldRateLimit();
    
    if (shouldLimit) {
      // Defer write operation
      const deferredOp: DeferredWrite = {
        id: `update-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
        operation: 'update',
        path: documentPath,
        data: data,
        timestamp: Date.now(),
        customerId: customerId
      };
      
      await this.deferWrite(deferredOp);
      
      // Return a placeholder WriteResult
      return { writeTime: admin.firestore.Timestamp.now() } as WriteResult;
    }
    
    try {
      const docRef = this.doc(documentPath);
      const result = await docRef.update(data);
      return result;
    } catch (error) {
      await this.handleError(error as Error, {
        operation: 'update',
        path: documentPath,
        data: data,
        timestamp: Date.now(),
        customerId: customerId,
        id: `error-${Date.now()}`
      });
      throw error;
    }
  }

  /**
   * Delete a document (write operation - rate limited)
   */
  public async delete(documentPath: string): Promise<WriteResult> {
    const customerId = this.currentCustomerId;
    
    // Check rate limit if enabled
    const shouldLimit = await this.shouldRateLimit();
    
    if (shouldLimit) {
      // Defer write operation
      const deferredOp: DeferredWrite = {
        id: `delete-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
        operation: 'delete',
        path: documentPath,
        timestamp: Date.now(),
        customerId: customerId
      };
      
      await this.deferWrite(deferredOp);
      
      // Return a placeholder WriteResult
      return { writeTime: admin.firestore.Timestamp.now() } as WriteResult;
    }
    
    try {
      const docRef = this.doc(documentPath);
      const result = await docRef.delete();
      return result;
    } catch (error) {
      await this.handleError(error as Error, {
        operation: 'delete',
        path: documentPath,
        timestamp: Date.now(),
        customerId: customerId,
        id: `error-${Date.now()}`
      });
      throw error;
    }
  }

  /**
   * Get a document (read operation - no rate limiting)
   */
  public async get(documentPath: string): Promise<DocumentSnapshot> {
    const docRef = this.doc(documentPath);
    return await docRef.get();
  }

  /**
   * Query documents in a collection (read operation - no rate limiting)
   */
  public async query(collectionPath: string, queryFn: (query: Query) => Query): Promise<QuerySnapshot> {
    const collectionRef = this.collection(collectionPath);
    const query = queryFn(collectionRef);
    return await query.get();
  }

  /**
   * Access the native Firestore instance directly if needed
   */
  public getNativeFirestore(): Firestore {
    return this.firestore;
  }

  /**
   * Create a batch operation with rate limiting support
   */
  public batch() {
    if (!this.rateLimiter || !this.deferredStorage) {
      // If no rate limiting or deferred storage, return native batch
      return this.firestore.batch();
    }
    
    // Create rate limited batch
    return new RateLimitedBatch(
      this.firestore,
      this.currentCustomerId,
      async (customerId, operationCount) => await this.shouldRateLimit(operationCount),
      async (batch) => {
        // Store batch for deferred processing
        // This is simplified - you'd want to adapt this to your storage system
        console.log(`Deferring batch with ${batch.operations.length} operations`);
        // Process each operation separately for now
        for (const op of batch.operations) {
          const deferredOp: DeferredWrite = {
            id: `batch-op-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
            operation: op.type as any,
            path: op.docPath,
            data: op.data,
            options: op.options,
            timestamp: Date.now(),
            customerId: batch.customerId
          };
          
          await this.deferWrite(deferredOp);
        }
      }
    }
}