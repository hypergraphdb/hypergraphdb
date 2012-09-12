/*
 * JVSTM: a Java library for Software Transactional Memory
 * Copyright (C) 2005 INESC-ID Software Engineering Group
 * http://www.esw.inesc-id.pt
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Author's contact:
 * INESC-ID Software Engineering Group
 * Rua Alves Redol 9
 * 1000 - 029 Lisboa
 * Portugal
 */
package org.hypergraphdb.transaction;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hypergraphdb.util.Cons;

public class ActiveTransactionsRecord
{
    /*
     * The ActiveTransactionsRecord class was designed as a lock-free data
     * structure. Its goal is to maintain a record of active transactions for
     * the purpose of garbage collecting old values.
     * 
     * It is composed of the following fields:
     */

    // the transactionNumber is assigned when the record is created and never
    // changes thereafter
    // its value corresponds to the number of a write-transaction that committed
    // already
    // this record (an instance of this class) is used to record all the running
    // transactions
    // that have this transaction number, and that, thus, may need to access the
    // values committed
    // by the transaction that created this record
    public final long transactionNumber;

    // the bodiesToGC field stores the bodies that were committed by the
    // transaction that
    // created this record
    // these are the bodies that must be GCed when no older active transactions
    // exist
    // this field is set to null when this record is cleaned-up
    // so, a null value in this field indicates that no transactions older than
    // this record exist
    private final AtomicReference<Cons<VBoxBody<?>>> bodiesToGC;

    // the running field indicates how many transactions with
    // this record's transactionNumber are still running
    private final AtomicInteger running = new AtomicInteger(0);

    // the next field indicates a more recent record (the one that was created
    // immediately after this one)
    // this field starts as null and is assigned exactly once, when a new
    // write-transaction commits, in
    // which case a new record is created
    private volatile ActiveTransactionsRecord next = null;
    
    // The original algorithm was modified to add a prev field so that a 
    // transaction record may be removed from the list of successors created
    // through the next field. This is necessary in order to support very long
    // transactions. Such transactions occur, for example, as large read-only
    // traversals or they may also happen in a distributed environment due 
    // to long network latencies.
    private volatile ActiveTransactionsRecord prev = null;
    
    /*
     * The next field creates, in effect, a linked list of records, with older
     * records pointing to newer records. This linked-list of records will allow
     * us to move forward in the cleaning process necessary to allow the garbage
     * collection of unreachable old-values.
     * 
     * The thread that is responsible for discarding the old-values made
     * obsolete by transaction number N (these values are kept in the bodiesToGC
     * field of the record with number N) is the thread of the last transaction
     * to finish that has a number less than N, provided that no new transaction
     * may start with a number less than N.
     * 
     * We must be sure, however, that we do not clean-up while there are still
     * old transactions running, and that we do not allow new transactions to
     * start with an old number that may be inactive already.
     * 
     * The algorithms in this class ensure that in a lock-free way. Let's see
     * how...
     * 
     * The fundamental fields used in these lock-free algorithms are the fields
     * "running" and "next". The access to these fields must be done in a
     * special order to ensure the correction of the algorithms.
     * 
     * But, before we look deeper into how these fields are accessed, we must
     * when is a record updated...
     * 
     * A record may be changed only in one of three situations:
     * 
     * 1. A new top-level transaction starts 2. A top-level write-transaction
     * commits 3. A top-level transaction finishes (either after commit, or on
     * abort)
     * 
     * The cases 1 and 3 seem simple: when a transaction starts, we must
     * increment the "running" counter, and when a transaction finishes, we must
     * decrement that same counter.
     * 
     * Case 2 corresponds to the creation of a new record and its linking to the
     * most recent one, via the "next" field of this latter record.
     * 
     * Finally, we must clean-up the records. When can we do it? The idea is
     * that a record may be cleaned-up when we are sure that there are no
     * running transactions older than the to-be-cleaned-up record
     * transactionNumber.
     * 
     * This is where the "running" field enters into play. When it reaches 0 for
     * a record with number N, we know that there are no transactions with
     * number N running. But that does not mean necessarily that there are no
     * transactions with a number less than N running, nor that no future
     * transactions may start with the number N.
     * 
     * To be able to clean-up records safely, we enforce the following
     * invariants:
     * 
     * 1. a record with number N is cleaned-up only when no transaction with a
     * number less than N is running
     * 
     * 2. no new transaction may start for a record that already has a non-null
     * value in its "next" field
     * 
     * 3. a record is cleaned-up when the last running transaction of one of its
     * predecessors finishes
     * 
     * A record may clean-up its successor when it has a non-null next value,
     * its running counter is 0, and it is clean already (meaning that there are
     * no previous running transactions).
     * 
     * So, in principle, whenever one of these conditions may change, we should
     * check whether we can clean-up the successor.
     * 
     * In fact, we may simplify the cases that we need to consider. The "next"
     * field is set only during the commit of a write transaction, when it
     * creates a new record. Yet, this committing transaction is still running,
     * and it has a number that is either equal to the record that will see its
     * next field changed (in which case, that record must have a running value
     * greater than 0), or it belongs to an older record (in which case, the
     * record with the next field updated cannot be clean yet). So, we do not
     * need to check whether a record may be cleaned-up when its "next" field is
     * updated.
     * 
     * This leaves us with the two remaining cases.
     * 
     * Yet, because the operations that may change any of these fields execute
     * concurrently, we must be careful on how we update and check the fields of
     * a record.
     * 
     * When checking whether a record may be clean, we *MUST* check first that
     * its "next" field is non-null. If that is true, then, because of the
     * invariant 2 stated above, we know that no new transactions may start at
     * this level. So, if the value of running is 0 and the record is itself
     * clean, we may clean the successor. If any of these two latter conditions
     * fail, then we do nothing, and whatever changes that must check again.
     * 
     * When a new transaction starts, it needs to increment the appropriate
     * running counter. It starts with the mostRecentRecord and speculatively
     * increments its running counter. By doing so, we prevent this record to be
     * cleaned-up if it has not been already, thereby avoiding problems with a
     * possible data-race on the updating of the various fields of the record.
     * Then, we must check whether the "next" field is non-null. If it is, we
     * must back-off of our speculative increment by decrementing the counter
     * again (which may trigger the cleaning-up process, because next is
     * non-null), and try again with the next record. This algorithm may cause
     * starvation on a thread that is trying to start a new transaction, but
     * only if successive write-transactions commit in between. Thus, this
     * algorithm is lock-free, rather than wait-free.
     * 
     * The final piece of all this is that when we clean-up a record, we should
     * try to propagate the cleaning process to its successor. This propagation
     * is accomplished by the method maybeCleanSuc that calls the clean method
     * successively for the various records.
     * 
     * To avoid propagating the cleaning multiple times by racing threads, the
     * clean method only allows cleaning a record once.
     */

    public ActiveTransactionsRecord(long txNumber, Cons<VBoxBody<?>> bodiesToGC)
    {
        this.transactionNumber = txNumber;
        this.bodiesToGC = new AtomicReference<Cons<VBoxBody<?>>>(bodiesToGC);
    }

    public void incrementRunning()
    {
        running.incrementAndGet();
    }

    public ActiveTransactionsRecord getNext()
    {
        return next;
    }

    protected void setPrev(ActiveTransactionsRecord prev)
    {
        this.prev = prev;
    }
    
    protected void setNext(ActiveTransactionsRecord next)
    {
        this.next = next;
        // we don't need to call the maybeCleanSuc() method after
        // setting the next field because this method is called only
        // during the commit of a write-transaction, in which case
        // that very same transaction is still running either in this
        // record or in some previous record. Therefore, it is not
        // possible to clean the successor yet.
    }

    /**
     * Return a record representing the latest committed transaction in a lock
     * free way. 
     */
    public ActiveTransactionsRecord getRecordForNewTransaction()
    {
        ActiveTransactionsRecord rec = this;
        while (true)
        {
            rec.running.incrementAndGet();
            if (rec.next == null)
            {
                // if there is no next yet, then it's because the rec
                // is the most recent one and we may return it
                return rec;
            }
            else
            {
                // a more recent record exists, so backoff and try
                // again with the new one
                rec.decrementRunning();
                rec = rec.next;
            }
        }
    }
    
    // see comment on 'prev' field on why this is being done
    // there's a potential race condition problem here because we're
    // doing a relatively sophisticated data structure management 
    // without synchronizing anything...but so far tests seem to pass
    // and the assumption is that we are modifying an inactive record
    // only by "writing" to the next/prev fields
    //
    // there's the potential of an NPE if, say, 'prev' becomes null after
    // the if has passed, but I want to see it happen before dealing with it :)
    public void maybeUnchain()
    {
        if (prev != null && next != null && running.get() == 0)
        {
            //System.out.println("Detaching record.");
            prev.next = next;
            next.prev = prev;
            this.prev = this.next = null;
        }        
    }
    
    public void decrementRunning()
    {
        if (running.decrementAndGet() == 0)
        {
            // when running reaches 0 maybe it's time to clean
            maybeCleanSuc();
            maybeUnchain();
        }
    }

   /* void maybeUnchainSuc()
    {
        // Try to dispose of intermediary records in the chain list that are done. Otherwise,
        // a long running transaction (i.e. couple of hours) coupled with millions of 
        // smaller transactions (e.g. cache purging) leads ot OutOfMemory because of millions
        // of chained records
        System.out.println("Checking rec.next");                    
        if (next.next != null && next.isClean() && next.running.get() == 0)
        {
            System.out.println("Try to dispose of rec.next!");
            if (next.next.clean())
            {
                System.out.println("Disposing of rec.next!");
                next = next.next;
            }
        }                
    } */
    
    private void maybeCleanSuc()
    {
        // it is crucial that we test the next field first, because
        // only after having the next non-null, do we have the
        // guarantee that no transactions may start for this record

        // if we checked the number of running first, it could happen
        // that no running existed, but one started between the test
        // for running and the test for next

        ActiveTransactionsRecord rec = this;
        while (true)
        {
            if ((rec.next != null /* must be first */) && rec.isClean()
                    && (rec.running.get() == 0))
            {
                if (rec.next.clean())
                {
                    // if we cleaned-up, try to clean-up further down the list                                        
                    rec = rec.next;
                    continue;
                }
            }
            break;
        }
    }

    private boolean isClean()
    {
        return (bodiesToGC.get() == null);
    }

    protected boolean clean()
    {
        Cons<VBoxBody<?>> toClean = bodiesToGC.getAndSet(null);

        // the toClean may be null because more than one thread may
        // race into this method
        // yet, because of the atomic getAndSet above, only one will
        // actually clean the bodies
        if (toClean != null)
        {
            for (VBoxBody<?> body : toClean)
            {
                body.clearPrevious();
            }
            return true;
        }
        else
        {
            return false;
        }
    }
}