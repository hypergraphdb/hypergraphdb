/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.transaction;

import java.util.HashMap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.event.HGTransactionEndEvent;
import org.hypergraphdb.util.Cons;
import org.hypergraphdb.util.Pair;

/**
 * 
 * <p>
 * Implements a transaction in HyperGraphDB.
 * </p>
 * 
 * <p>
 * Each transaction can carry an arbitrary set of attributes along with it. This
 * is useful for attaching contextual information to transactions without
 * intruding to otherwise simple APIs. When a transaction is committed or
 * aborted, all attributes are removed.
 * </p>
 * 
 * @author Borislav Iordanov
 * 
 */
@SuppressWarnings("unchecked")
public final class HGTransaction implements HGStorageTransaction
{
    static final Object NULL_VALUE = new Object();

    private HGTransaction parent;
    private HGTransactionContext context;
    private HGStorageTransaction stran = null;
    private HashMap<Object, Object> attributes = new HashMap<Object, Object>();
    private Set<Pair<VBox<?>, VBoxBody<?>>> bodiesRead = new HashSet<Pair<VBox<?>, VBoxBody<?>>>();
    private Map<VBox<?>, Object> boxesWritten = new HashMap<VBox<?>, Object>();
    private long number;
    private ActiveTransactionsRecord activeTxRecord;
    
    long getNumber()
    {
        return number;
    }
    
    <T> T getLocalValue(VBox<T> vbox)
    {
        T value = null;
        HGTransaction tx = this;
        do
        {
            value = (T) tx.boxesWritten.get(vbox);
        } while (value == null && (tx = tx.parent) != null);
        return value;
    }

    <T> T getBoxValue(VBox<T> vbox)
    {
        T value = getLocalValue(vbox);
        if (value == null)
        {
            VBoxBody<T> body = vbox.body.getBody(number);
            bodiesRead.add(new Pair<VBox<?>, VBoxBody<?>>(vbox, body));
            value = body.value;
        }
        return (value == NULL_VALUE) ? null : value;
    }

    <T> void setBoxValue(VBox<T> vbox, T value)
    {
        boxesWritten.put(vbox, value == null ? NULL_VALUE : value);
    }

    private boolean isWriteTransaction()
    {
        return !boxesWritten.isEmpty();
    }

    /**
     * A commit can proceed only if none of the values we've read during
     * the transaction has changed (i.e. has been committed) since we read
     * them. In order words, the latest/current body of each VBox is the same
     * as the one tagged with this transaction's number.
     */
    protected boolean validateCommit()
    {
        for (Pair<VBox<?>, VBoxBody<?>> entry : bodiesRead)
        {
            if (entry.getFirst().body != entry.getSecond())
            {
                return false;
            }
        }
        return true;
    }

    protected Cons<VBoxBody<?>> performValidCommit()
    {
        number = context.getManager().mostRecentRecord.transactionNumber + 1;
        return doCommit();
    }

    protected Cons<VBoxBody<?>> doCommit()
    {
        Cons<VBoxBody<?>> newBodies = Cons.EMPTY;

        for (Map.Entry<VBox<?>, Object> entry : boxesWritten.entrySet())
        {
            VBox<Object> vbox = (VBox<Object>)entry.getKey();
            Object newValue = entry.getValue();

            VBoxBody<?> newBody = vbox.commit(this, (newValue == NULL_VALUE) ? null
                                    : newValue, number);
            newBodies = newBodies.cons(newBody);
        }

        return newBodies;
    }

    void finish() 
    {
        for (Map.Entry<VBox<?>, Object> entry : boxesWritten.entrySet())
            entry.getKey().finish();
        bodiesRead = null;
        boxesWritten = null;        
        activeTxRecord.decrementRunning();
    }
    
    HGTransaction(HGTransactionContext context, 
                  HGTransaction parent,
                  ActiveTransactionsRecord activeTxRecord, 
                  HGStorageTransaction impl)
    {
        this.stran = impl;
        this.context = context;
        this.parent = parent;
        this.activeTxRecord = activeTxRecord;
        this.number = activeTxRecord.transactionNumber;
    }

    public HGStorageTransaction getStorageTransaction()
    {
        return stran;
    }

    public void commit() throws HGTransactionException
    {
        // If this is a nested transaction, everything is much simpler
        if (parent != null)
        {
            stran.commit();            
            parent.bodiesRead.addAll(bodiesRead);
            parent.boxesWritten.putAll(boxesWritten);
            finish();
            HyperGraph graph = context.getManager().getHyperGraph();
            graph.getEventManager().dispatch(graph,
                                             new HGTransactionEndEvent(this, true));            
            return;
        }
        
        // Otherwise this is a top-level transaction, we need to do more serious work.
        
        if (isWriteTransaction())
        {
            context.getManager().COMMIT_LOCK.lock();
            try
            {
                if (validateCommit())
                {
                    stran.commit();
                    Cons<VBoxBody<?>> bodiesCommitted = performValidCommit();
                    
                    // The commit is already done, so create a new ActiveTransactionsRecord
                    ActiveTransactionsRecord newRecord = new ActiveTransactionsRecord(number,
                                                                                      bodiesCommitted);
                    context.getManager().mostRecentRecord.setNext(newRecord);
                    context.getManager().mostRecentRecord = newRecord;
                    
                    // as this transaction changed number, we must
                    // update the activeRecords accordingly

                    // the correct order is to increment first the
                    // new, and only then decrement the old
                    newRecord.incrementRunning();
                    this.activeTxRecord.decrementRunning();
                    
                    // This assignment is need to decrementRunning in the finish method below.                    
                    this.activeTxRecord = newRecord;
                }
                else
                {
//                    System.out.println("Transaction conflict.");
                    privateAbort();
                    throw new TransactionConflictException();
                }
            }
            finally
            {                
                context.getManager().COMMIT_LOCK.unlock();                
            }
        }                
        else
            stran.commit();
        finish();            
        HyperGraph graph = context.getManager().getHyperGraph();
        graph.getEventManager().dispatch(graph,
                                         new HGTransactionEndEvent(this, true));
//        System.out.println("Transaction succeeded.");
    }

    private void privateAbort() throws HGTransactionException
    {
        finish();
        stran.abort();        
    }
    
    public void abort() throws HGTransactionException
    {
        privateAbort();
        HyperGraph graph = context.getManager().getHyperGraph();
        graph.getEventManager().dispatch(graph, 
                                         new HGTransactionEndEvent(this, false));
    }

    public <T> T getAttribute(Object key)
    {
        return (T)attributes.get(key);
    }

    public Iterator<Object> getAttributeKeys()
    {
        return attributes.keySet().iterator();
    }

    public void removeAttribute(Object key)
    {
        attributes.remove(key);
    }

    public void setAttribute(Object key, Object value)
    {
        attributes.put(key, value);
    }
}
