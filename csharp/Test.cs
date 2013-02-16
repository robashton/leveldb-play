using System;
using System.Threading;
using System.Linq;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using NUnit.Framework;

public class StorageTransaction {

  private int id;

  private List<Action<Storage>> operations = new List<Action<Storage>>();
  private StorageTransaction[] trackedTransactions;
  private int transactionCountTrackingMe = 1;
  

  public int Id { get { return id; }}
  public int RefCount { get { return transactionCountTrackingMe; } }

  public StorageTransaction(int id, StorageTransaction[] activeTransactions) {
    this.id = id;
    this.trackedTransactions = activeTransactions;
    foreach(var otherTransaction in this.trackedTransactions) {
      otherTransaction.IncreaseRefCount();
    }
  }

  internal void AddOperation(Action<Storage> operation) {
    operations.Add(operation);
  }

  internal void Rollback() {
    this.Complete();
  }

  internal void Commit(Storage storage) {
    try {
      this.operations.ForEach(action => action(storage));
    }
    finally {
      this.Complete();
    }
  }

  private void Complete() {
    this.DecreaseRefCount();
    foreach(var otherTransaction in this.trackedTransactions) {
      otherTransaction.DecreaseRefCount();
    }
  }

  internal void IncreaseRefCount() {
    Interlocked.Increment(ref this.transactionCountTrackingMe);
  }

  internal void DecreaseRefCount() {
    Interlocked.Decrement(ref this.transactionCountTrackingMe);
  }
}

public class Storage {
  private ConcurrentDictionary<string, object> backing = new ConcurrentDictionary<string, object>();
  private ConcurrentDictionary<string, int> keysToTransactionId = new ConcurrentDictionary<string, int>();
  private ConcurrentDictionary<int, StorageTransaction> activeTransactions = new ConcurrentDictionary<int, StorageTransaction>();

  private int lastTransactionId = 0;

  public Storage() {

  }

  public void Batch(Action<StorageAccessor> actions) {
    var transaction = this.CreateTransaction();
    var accessor = new StorageAccessor(this, transaction);
    try {
      actions(accessor);
    } catch(Exception ex) {
      this.RollbackTransaction(transaction);
      throw;
    }
    this.CommitTransaction(transaction);
  }

  private StorageTransaction CreateTransaction() {
    Interlocked.Increment(ref this.lastTransactionId);
    var currentTransactions = this.activeTransactions.ToArray();
    var transaction = new StorageTransaction(this.lastTransactionId, currentTransactions.Select(x=> x.Value).ToArray());
    this.activeTransactions.TryAdd(this.lastTransactionId, transaction);
    return transaction;
  }

  private void RollbackTransaction(StorageTransaction transaction) {
    transaction.Rollback();
    this.TryPruneObsoleteTransactions();
  }

  private void CommitTransaction(StorageTransaction transaction) {
    // This would be LevelDB.BatchWrite, I just use a database wide lock, mileage may vary
    lock(this.backing) {
      transaction.Commit(this);
    }
    this.TryPruneObsoleteTransactions();
  }

  private void TryPruneObsoleteTransactions() {
    var currentTransactions = this.activeTransactions.Values.OrderBy(x=> x.Id).ToArray();

    var transactionsWeCanRemove = new List<int>();
    foreach(var transaction in currentTransactions) {
      if(transaction.RefCount == 0)
        transactionsWeCanRemove.Add(transaction.Id);
      else
        break;
    }

    List<string> keysToClear = new List<string>();

    foreach(var kv in this.keysToTransactionId) {
      if(transactionsWeCanRemove.Contains(kv.Value))
        keysToClear.Add(kv.Key);
    }

    keysToClear.ForEach(key=> {
      int ignored;
      this.keysToTransactionId.TryRemove(key, out ignored);
    });

    transactionsWeCanRemove.ForEach(key => {
      StorageTransaction ignored;
      this.activeTransactions.TryRemove(key, out ignored);
    });
  }

  public Object Get(string id, StorageTransaction transaction) {
    // This would ordinarily be using a Snapshot for the transaction in LevelDB
    Object outValue;
    if(this.backing.TryGetValue(id, out outValue))
      return outValue;
    return null;
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


[TestFixture]
public class Tests
{
  [Test]
  public void Can_store_single_document() {
    var storage = new Storage();
    storage.Batch(x=> x.Put("1", "Hello"));

    string doc = null;
    storage.Batch(x=> {
      doc = (string)x.Get("1");
    });
    Assert.That(doc, Is.EqualTo("Hello"));
  }

  [Test]
  public void Can_store_multiple_document() {
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
  public void Can_delete_single_document() {
    var storage = new Storage();
    storage.Batch(x=> x.Put("1", "Hello"));
    storage.Batch(x=> x.Delete("1"));
    string doc = null;
    storage.Batch(x=> {
      doc = (string)x.Get("1");
    });
    Assert.That(doc, Is.EqualTo(null));
  }

  [Test]
  public void Deleting_updating_document_before_update_transaction_is_closed_throws_exception() {
    var storage = new Storage();
    storage.Batch(x=> x.Put("1", "Hello"));
    Exception thrown = null;
    storage.Batch(x=> {
      x.Put("1", "Overwrite");
      try {
        storage.Batch(y=> y.Delete("1"));
      } catch(Exception ex) {
        thrown = ex;
      }
    });
    Assert.NotNull(thrown);
  }

  [Test]
  public void Putting_two_documents_with_same_key_in_overlapping_transactions_throws_exception() {
    var storage = new Storage();
    Exception thrown = null;
    storage.Batch(x=> {
      x.Put("1", "Hello");
      try {
        storage.Batch(y=> y.Put("1", "Another"));
      } catch(Exception ex) {
        thrown = ex;
      }
    }); 
    Assert.NotNull(thrown);
  }

  [Test]
  public void A_concurrency_exception_leaves_the_database_in_a_usable_state() {
    var storage = new Storage();
    Exception thrown = null;
    storage.Batch(x=> {
      x.Put("1", "Hello");
      try {
        storage.Batch(y=> y.Put("1", "Another"));
      } catch(Exception ex) {
        thrown = ex;
      }
    }); 
    storage.Batch(x=> x.Put("1", "Replaced"));
    string doc = null;
    storage.Batch(x=> {
      doc = (string)x.Get("1");
    });
    Assert.That(doc, Is.EqualTo("Replaced"));
    Assert.NotNull(thrown);
  }
}
