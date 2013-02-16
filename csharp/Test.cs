using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using NUnit.Framework;

public class StorageTransaction {

  private int id;
  private List<Action<Storage>> operations = new List<Action<Storage>>();

  public int Id { get { return id; }}

  public StorageTransaction(int id) {
    this.id = id;
  }


  internal void AddOperation(Action<Storage> operation) {
    operations.Add(operation);
  }


  internal void Commit(Storage storage) {
    // Whatevs, LevelDB would handle failure for us here
    this.operations.ForEach(action => action(storage));
  }
}

public class Storage {
  private ConcurrentDictionary<string, object> backing = new ConcurrentDictionary<string, object>();
  private ConcurrentDictionary<string, int> keysToTransactionId = new ConcurrentDictionary<string, int>();
  private int lastTransactionId = 0;

  public Storage() {

  }

  public void Batch(Action<StorageAccessor> actions) {
    var transaction = this.CreateTransaction();
    var accessor = new StorageAccessor(this, transaction);
    actions(accessor);
    this.CommitTransaction(transaction);
  }

  private StorageTransaction CreateTransaction() {
    var transaction = new StorageTransaction(++this.lastTransactionId);
    return transaction;
  }

  private void CommitTransaction(StorageTransaction transaction) {
    // LevelDB.BatchWrite
    lock(this.backing) {
      transaction.Commit(this);
    }
  }


  public Object Get(string id, StorageTransaction transaction) {
    // This would ordinarily be using a Snapshot for the transaction in LevelDB
    return this.backing[id];
  }

  public void Put(string id, Object obj, StorageTransaction transaction) {
    this.keysToTransactionId.AddOrUpdate(id, (key) => {
        transaction.AddOperation(storage => storage.Put(id, obj));
        return transaction.Id;
      }, 
      (key, oldValue) => {
      // NOTE: This doesn't handle the transaction doing multiple operations on the same key
      throw new Exception("This should be a concurrency exception");
    });
  }

  public void Delete(string id, StorageTransaction transaction) {
    this.keysToTransactionId.AddOrUpdate(id, (key) => {
        transaction.AddOperation(storage => storage.Delete(id));
        return transaction.Id;
      }, 
      (key, oldValue) => {
      // NOTE: This doesn't handle the transaction doing multiple operations on the same key
      throw new Exception("This should be a concurrency exception");
    });
  }

  internal void Delete(string id) {
    object ignored;
    this.backing.TryRemove(id, out ignored);
  }

  internal void Put(string id, Object obj) {
    this.backing[id] = obj;
  }
}


public class StorageAccessor {
  private Storage storage;
  private StorageTransaction transaction;

  public StorageAccessor(Storage storage, StorageTransaction transaction) {
    this.storage = storage;
    this.transaction = transaction;
  }

  public Object Get(string id) {
    return this.storage.Get(id, this.transaction);
  }

  public void Put(string id, Object obj) {
    this.storage.Put(id, obj, this.transaction);
  }

  public void Delete(string id) {
    this.storage.Delete(id, this.transaction);
  }
}

public static class Program {
  public static int Main(String[] args) {

    var storage = new Storage();

    storage.Batch(x=> {

    });


    return 0;
  }
}

[TestFixture]
public class Tests
{
  [Test]
  public void Can_Store_Single_Document() {
    var storage = new Storage();
    storage.Batch(x=> x.Put("1", "Hello"));

    string doc = null;
    storage.Batch(x=> {
      doc = (string)x.Get("1");
    });
    Assert.That(doc, Is.EqualTo("Hello"));
  }

  [Test]
  public void Can_Store_Multiple_Document() {
    var storage = new Storage();
    storage.Batch(x=>  {
      x.Put("1", "Hello");
      x.Put("2", "Hello World");
    });

    string doc1 = null;
    string doc2 = null;
    storage.Batch(x=> {
      doc1 = (string)x.Get("1");
      doc2 = (string)x.Get("2");
    });
    Assert.That(doc1, Is.EqualTo("Hello"));
    Assert.That(doc2, Is.EqualTo("Hello World"));
  }

  [Test]
  public void Can_Delete_Single_Document() {
    var storage = new Storage();
    storage.Batch(x=> x.Put("1", "Hello"));
    storage.Batch(x=> x.Delete("1"));
    string doc = null;
    storage.Batch(x=> {
      doc = (string)x.Get("1");
    });
    Assert.That(doc, null);
  }
}
