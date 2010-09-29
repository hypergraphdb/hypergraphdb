package org.hypergraphdb.transaction;

import java.util.List;
import org.hypergraphdb.util.HGSortedSet;
import org.hypergraphdb.util.RefCountedMap;
import org.hypergraphdb.util.RefResolver;

public class TxCacheSet<Key, E> extends TxSet<E>
{
    private Key key;
    private RefCountedMap<Key, VBox<HGSortedSet<E>>> writeMap;    
    private RefResolver<Key, ? extends HGSortedSet<E>> loader; 
    
    void insertBody(long txNumber, HGSortedSet<E> x)
    {
        txManager.COMMIT_LOCK.lock();
        try
        {
            if (txNumber >= txManager.mostRecentRecord.transactionNumber) // is this the latest value
            {
                // a marker that we have loaded some older values for older, but still running tx
                if (S.body.value == null)
                {
                    S.body = S.makeNewBody(x, txNumber, S.body.next);
                }                
                else // otherwise we just add as the newest body with the current tx number
                    S.body = S.makeNewBody(x, txNumber, S.body);
            }
            // if not current, we must insert it into the list of bodies and make sure
            // the top body is "null" in order to force a subsequent reload
            else
            {
                // make sure top body is null
                if (S.body.value != null)
                    S.body = S.makeNewBody(null, txManager.mostRecentRecord.transactionNumber, S.body);
                
                VBoxBody<HGSortedSet<E>> currentBody = S.body;
                while (currentBody.next != null && currentBody.next.version > txNumber)
                    currentBody = currentBody.next;
                // we need to insert b/w currentBody and currentBody.next
                VBoxBody<HGSortedSet<E>> newBody = S.makeNewBody(x, txNumber, currentBody.next);
                currentBody.setNext(newBody);                
            }               
        }
        finally
        {
            txManager.COMMIT_LOCK.unlock();
        }        
    }
    
    HGSortedSet<E> load(long txNumber)
    {
        HGSortedSet<E> x = loader.resolve(key);
        insertBody(txNumber, x);
        return x;
    }
    
    @Override
    HGSortedSet<E> read()
    {
        HGTransaction tx = txManager.getContext().getCurrent();
        HGSortedSet<E> x = tx.getLocalValue(S);        
        if (x == null) // no local value, we get the correct version if loaded or return null if not
        {
            VBoxBody<HGSortedSet<E>> b = S.body;
            // if the current transaction is not older than the top body we return it:
            // it will be null if we need a load from disk or the latest committed value
            // which would be the correct version
            if (b.version <= tx.getNumber())
                return b.value == null ? load(tx.getNumber()) : b.value;
            else
            {
                // else try to find the exact same version as the current transaction                
                while (b.version > tx.getNumber() && b.next != null)
                    b = b.next;
                if (b.version == tx.getNumber())
                    return b.value;
                else
                    return load(tx.getNumber());
            }            
        }
        else 
            return x;        
    }
    
    @Override
    HGSortedSet<E> write()
    {
        List<LogEntry> log = txManager.getContext().getCurrent().getAttribute(S);
        if (log == null) // should we copy-on-write?
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
        S = writeMap.get(key);
        long txNumber = txManager.getContext().getCurrent().getNumber();
        if (S == null)
        {
            S = new CacheSetTxBox<Key, E>(txManager, backingSet, this);
            
            // we need to tag the body with the current transaction's version
            // since body.version is final, we replace with a new body
            S.body = S.makeNewBody(backingSet, txNumber, null);
            // if this is an old transaction, we need to put null at the top of the body
            // list so the latest will get reloaded if needed
            if (txNumber < txManager.mostRecentRecord.transactionNumber)
                S.body = S.makeNewBody(null, txManager.mostRecentRecord.transactionNumber, S.body);
        }
        else
            insertBody(txNumber, backingSet);
    }   
    
    public static class CacheSetTxBox<Key, E> extends SetTxBox<E>
    {
        CacheSetTxBox(final HGTransactionManager txManager, 
                      final HGSortedSet<E> backingSet,
                      final TxSet<E> thisSet)
                 {
                    super(txManager, backingSet, thisSet);
                 }
        
        @SuppressWarnings("unchecked")
        @Override
        public VBoxBody<HGSortedSet<E>> commit(HGTransaction tx, HGSortedSet<E> newvalue, long txNumber)
        {
            if (tx.getAttribute(thisSet) != null)
            {
                TxCacheSet<Key, E> s = (TxCacheSet<Key, E>)thisSet;  
                s.writeMap.remove(s.key);
            }
            return super.commit(tx, newvalue, txNumber);
        }
    }
}