import { MongoClient, Db } from 'mongodb';

export async function connect(uri: string) {
  const client = new MongoClient(uri, { maxPoolSize: 20 });
  await client.connect();
  return client;
}

export async function close(client: MongoClient) {
  try {
    await client.close();
  } catch {
    // swallow
  }
}

export async function getDb(client: MongoClient, dbName: string): Promise<Db> {
  return client.db(dbName);
}
