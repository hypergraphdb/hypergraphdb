package org.hypergraphdb.transaction;

import java.util.List;

import org.hypergraphdb.util.HGSortedSet;
import org.hypergraphdb.util.RefCountedMap;
import org.hypergraphdb.util.RefResolver;

public class TxCacheSet<Key, E> extends TxSet<E>
{
    private Key key;
    private RefCountedMap<Key,SetTxBox<E>> writeMap;    
    private RefResolver<Key, ? extends HGSortedSet<E>> loader; 
    
    @SuppressWarnings("unchecked")
    VBoxBody<HGSortedSet<E>> insertBody(long txNumber, HGSortedSet<E> x)
    {
        txManager.COMMIT_LOCK.lock();
        try
        {
            if (txNumber >= txManager.mostRecentRecord.transactionNumber) // is this the latest value
            {
                // a marker that we have loaded some older values for older, but still running tx
                if (S.body.version == -1)
                {
                    S.body = S.makeNewBody(x, txNumber, S.body.next);
                }                
                else if (S.body.version < txNumber) // otherwise we just add as the newest body with the current tx number
                    S.body = S.makeNewBody(x, txNumber, S.body);
                return S.body;
            }
            // if not current, we must insert it into the list of bodies 
            else
            {
                if (S.body.version == -1)
                {
                    // We want to avoid endlessly loading the same version from disk in case
                    // we always load into a transaction that doesn't have the latest number, but
                    // the value hasn't actually changed. The 'S.loadedAt' field marks the transaction
                    // number when the box was first loaded so if there are no commits between S.loadedAt
                    // and now (which a top version of -1 indicates) then the latest possible version 
                    // is S.loadedAt. So if txNumber>=S.loadedAt we should
                    // mark this version as the latest
                    if (txNumber >= ((CacheSetTxBox<Key, E>)S).loadedAt)
                    {
                        S.body = S.makeNewBody(x, ((CacheSetTxBox<Key, E>)S).loadedAt, S.body.next);
                        return S.body;
                    }
                }
                
                VBoxBody<HGSortedSet<E>> currentBody = S.body;
                while (currentBody.next != null && currentBody.next.version > txNumber)
                    currentBody = currentBody.next;
                // Could happen that we unnecessarily loaded the same set twice due to race conditions
                // so at least we avoid storing it twice.
                if (currentBody.next != null && currentBody.next.version == txNumber)
                    return currentBody.next;
                // we need to insert b/w currentBody and currentBody.next
                VBoxBody<HGSortedSet<E>> newBody = S.makeNewBody(x, txNumber, currentBody.next);
                currentBody.setNext(newBody);
                return newBody;
            }               
        }
        finally
        {
            txManager.COMMIT_LOCK.unlock();
        }        
    }
    
    VBoxBody<HGSortedSet<E>> load(long txNumber)
    {
        HGSortedSet<E> x = loader.resolve(key);
        return insertBody(txNumber, x);
        //return x;
    }
    
    @Override
    HGSortedSet<E> read()
    {
        HGTransaction tx = txManager.getContext().getCurrent();
        if (tx == null)
            return S.body.value;
        HGSortedSet<E> x = tx.getLocalValue(S);        
        if (x == null) // no local value, we get the correct version if loaded or return null if not
        {
            VBoxBody<HGSortedSet<E>> b = S.body;
            // if the current transaction is not older than the top body we return it:
            // it will be null if we need a load from disk or the latest committed value
            // which would be the correct version
            if (b.version <= tx.getNumber())
            {
                if (b.value == null)
                    b = load(tx.getNumber());
            }
            else
            {
                // else try to find the exact same version as the current transaction                
                while (b.version > tx.getNumber() && b.next != null)
                    b = b.next;
                if (b.version != tx.getNumber())
                    b = load(tx.getNumber());
            }            
            if (!tx.isReadOnly())
                tx.bodiesRead.put(S, b);
            return b.value;
        }
        else 
        {
            return x == HGTransaction.NULL_VALUE ? null : x;
        }
    }
    
    @Override
    HGSortedSet<E> write()
    {
        List<LogEntry> log = txManager.getContext().getCurrent().getAttribute(S);
        if (log == null) // should we copy-on-write or have we done so already?
        {
            HGSortedSet<E> readOnly = read(); // S.getForWrite();
            HGSortedSet<E> writeable = cloneSet(readOnly);
            S.put(writeable);
            writeMap.put(key, S);
        }
        return S.get();
    }
    
    public TxCacheSet(final HGTransactionManager txManager, 
                      final HGSortedSet<E> backingSet, 
                      final Key key,
                      final RefResolver<Key, ? extends HGSortedSet<E>> loader,
                      final RefCountedMap<Key, SetTxBox<E>> writeMap)
    {
        this.txManager = txManager;
        this.key = key;
        this.loader = loader;
        this.writeMap = writeMap;
        HGTransaction tx = txManager.getContext().getCurrent();
        if (tx == null)
        {
            S = new CacheSetTxBox<Key, E>(txManager, backingSet, this);
            return;
        }        
        S = writeMap.get(key);         
        long txNumber = tx.getNumber();
        if (S == null)
        {
            S = new CacheSetTxBox<Key, E>(txManager, backingSet, this);
            
            // we need to tag the body with the current transaction's version
            // since body.version is final, we replace with a new body
            S.body = S.makeNewBody(backingSet, txNumber, null); 
            // if this is an old transaction, we need to put null at the top of the body
            // list so the latest will get reloaded if needed
            if (txNumber < txManager.mostRecentRecord.transactionNumber)
                S.body = S.makeNewBody(null, -1, S.body);
        }
        else
            insertBody(txNumber, backingSet);
    }   
    
    public static class CacheSetTxBox<Key, E> extends SetTxBox<E>
    {
        long loadedAt;
        
        CacheSetTxBox(final HGTransactionManager txManager, 
                      final HGSortedSet<E> backingSet,
                      final TxSet<E> thisSet)
        {
            super(txManager, backingSet, thisSet);
            loadedAt = txManager.mostRecentRecord.transactionNumber;
        }
        
        @SuppressWarnings("unchecked")
        HGSortedSet<E> getLastCommitted(HGTransaction tx)
        {
            TxCacheSet<Key, E> s = (TxCacheSet<Key, E>)thisSet;            
            HGSortedSet<E> lastCommitted = super.getLastCommitted(tx);                
            return  (lastCommitted == null) ?  s.load(tx.getNumber()).value : lastCommitted;
        }
        
        @Override
        public VBoxBody<HGSortedSet<E>> commit(HGTransaction tx, HGSortedSet<E> newvalue, long txNumber)
        {
            VBoxBody<HGSortedSet<E>> latest = super.commit(tx, newvalue, txNumber);
            // check if we have the special "old value" marker hanging in the second place of
            // the list of bodies after the commit, and if so unlink it
            if (latest.next != null && latest.next.version == -1)
                latest.setNext(latest.next.next);
            return latest;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void finish(HGTransaction tx)
        {
            if (tx.getAttribute(this) != null)
            {
                TxCacheSet<Key, E> s = (TxCacheSet<Key, E>)thisSet;                
                s.writeMap.remove(s.key);
            }
        }        
    }
}