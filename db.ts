import admin, { firestore } from "firebase-admin";
import { AddPrefixToKeys } from "firebase-admin/firestore";

export interface User {
  id: string;
  name: string;
  email: string;
  age: number;
  status: number;
}

export interface VaultBalance {
  asset: string;
  total: string;
  pending: string;
  frozen: string;
  locked: string;
  staked?: string;
  pendingRewards?: string;
  totalStakedCpu?: string;
  totalStakedNetwork?: string;
}

type VaultWalletBalance = {
  balance: string;
  pendingBalance: string;
  frozenBalance: string;
  lockedAmount: string;
  stakedBalance?: string;
  totalStakedCpu?: string;
  totalStakedNetwork?: string;
  pendingRewards?: string;
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

  public async updateVaultWalletBalance(tenantId: string, accountId: string, balance: Partial<VaultBalance> & { asset: string }): Promise<void> {
    console.log(`In updateVaultWalletBalance, tenantId: ${tenantId}, asset: ${JSON.stringify(balance)}`);
    await this.updateVaultWalletBalanceDoc(tenantId, accountId, balance);
    await this.updateVaultWalletBalanceSubCollection(tenantId, accountId, balance);
    console.log(`Vault wallet balance updated for tenantId: ${tenantId}, accountId: ${accountId}, asset: ${balance.asset}`);
  }

  private async updateVaultWalletBalanceDoc(tenantId: string, accountId: string, balance: Partial<VaultBalance> & { asset: string }): Promise<void> {
    console.log(`In updateVaultWalletBalanceDoc, tenantId: ${tenantId}, asset: ${JSON.stringify(balance)}`);

    const update: AddPrefixToKeys<`wallets.${string}`, VaultWalletBalance> = {
      [`wallets.${balance.asset}.balance`]: balance.total,
      [`wallets.${balance.asset}.pendingBalance`]: balance.pending,
      [`wallets.${balance.asset}.frozenBalance`]: balance.frozen,
      [`wallets.${balance.asset}.lockedAmount`]: balance.locked,
    };

    if (balance.staked) {
      update[`wallets.${balance.asset}.stakedBalance`] = balance.staked;
    }

    if (balance.totalStakedCpu) {
      update[`wallets.${balance.asset}.totalStakedCpu`] = balance.totalStakedCpu;
    }

    if (balance.totalStakedNetwork) {
      update[`wallets.${balance.asset}.totalStakedNetwork`] = balance.totalStakedNetwork;
    }

    if (balance.pendingRewards) {
      update[`wallets.${balance.asset}.pendingRewards`] = balance.pendingRewards;
    }

    await this.db.collection('tenants').doc(tenantId).collection('vaultAccounts').doc(accountId).update(update);
  }

  private async updateVaultWalletBalanceSubCollection(tenantId: string, accountId: string, balance: Partial<VaultBalance> & { asset: string }): Promise<void> {
    console.log(`In updateVaultWalletBalanceSubCollection, tenantId: ${tenantId}, asset: ${JSON.stringify(balance)}`);
    const update: Partial<VaultWalletBalance> = {
      balance: balance.total,
      pendingBalance: balance.pending,
      frozenBalance: balance.frozen,
      lockedAmount: balance.locked,
    };
    if (balance.staked) {
      update.stakedBalance = balance.staked;
    }
    if (balance.totalStakedCpu) {
      update.totalStakedCpu = balance.totalStakedCpu;
    }
    if (balance.totalStakedNetwork) {
      update.totalStakedNetwork = balance.totalStakedNetwork;
    }
    if (balance.pendingRewards) {
      update.pendingRewards = balance.pendingRewards;
    }
    const doc = this.db.collection('tenants').doc(tenantId).collection('vaultAccounts').doc(accountId);
    const assetWalletRef = doc.collection('wallets').doc(balance.asset);
    const assetWallet = await assetWalletRef.get();
    if (assetWallet.exists) {
      await assetWalletRef.update({ ...update });
    } else {
      await assetWalletRef.create(update);
    }
  }

  // TODO: Think about moving this to subCollection? 
  public async getVaultWallets(tenantId: string, vaultAccountId: string): Promise<string[]> {
    // Add FF guard for sub-collection usage
    if (true) {
      // @ts-ignore
      return this.getVaultWalletsFromSubCollection(tenantId, vaultAccountId);
    }
    // @ts-ignore
    return this.getVaultWalletsFromDoc(tenantId, vaultAccountId);
  }

  public async getVaultWalletsFromDoc(tenantId: string, vaultAccountId: string): Promise<string[]> {
    const data = (
      await this.db.collection("tenants").doc(tenantId).collection('vaultAccounts').doc(vaultAccountId).get()
    ).data();
    return Object.keys(data?.wallets);
  }

  public async getVaultWalletsFromSubCollection(tenantId: string, vaultAccountId: string): Promise<string[]> {
    const snapshot = await this.db.collection("tenants").doc(tenantId).collection('vaultAccounts').doc(vaultAccountId).collection('wallets').listDocuments();
    const wallets = snapshot.map(doc => {
      return doc.id;
    });
    return wallets;
  }
}

export default FirestoreDB;
