package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.transaction.HGStorageTransaction
import com.hazelcast.core.Transaction

class HazelTransaction(t:Transaction) extends HGStorageTransaction{

  //  def begin { if (t.getStatus == 0) t.begin }

  def commit() {
    val stat = t.getStatus
    if(stat == 1 || stat == 2 || stat == 5) // ToDo -- check if this is ok to handle nested Transactions!
      t.commit()
  }

  def abort() {
    val stat = t.getStatus
    if(stat == 1 || stat == 6 || stat == 2 || stat == 5)
      t.rollback()
  }

  def status:Int = t.getStatus
}
