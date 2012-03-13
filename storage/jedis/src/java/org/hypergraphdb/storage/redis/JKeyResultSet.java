package org.hypergraphdb.storage.redis;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;

import java.util.Set;

public class JKeyResultSet<KeyType> extends JIndexResultSet<KeyType> {
    private JKeyResultSetImpl<KeyType> impl;
    private int[] currentPosition = new int[1];
    private int indexID;
    private JedisStore storage;
    private ByteArrayConverter<KeyType> keyConverter;
    private boolean useCache;

    public JKeyResultSet(JedisStore storage, int indexID, Set<byte[]> resultKeySet, ByteArrayConverter<KeyType> keyConverter, boolean useCache) {
        this.indexID = indexID;
        this.storage = storage;
        this.keyConverter = keyConverter;
        this.useCache = useCache;
        this.impl = JKeyResultSetImpl.getJKeyResultSetImpl(storage, indexID,resultKeySet, keyConverter, useCache);
        this.currentPosition[0] = -1;
    }

    public GotoResult goTo(KeyType value, boolean exactMatch)
        {
            GotoResult result = GotoResult.nothing;
            int[] goToResult= impl.goTo(value, exactMatch);
            if (goToResult[1] == -1)
                  return GotoResult.nothing;
            if (goToResult[1] == 0)
            {     currentPosition[0]=goToResult[0];
                  result = GotoResult.found;
            }
            if (goToResult[1] == 1)
            {     currentPosition[0]=goToResult[0];
                  result = GotoResult.close;
            }
            return result;
        }

    public void goAfterLast()   { currentPosition[0] = -2; }

    public void goBeforeFirst() { currentPosition[0] = -1;}

    public KeyType current()    { return impl.current(currentPosition);}

    public void close()         { }

    public boolean isOrdered()  { return true; }            // TODO -- check isOrdered

    public boolean hasPrev()    { return impl.hasPrev(currentPosition); }

    public KeyType prev()       { return impl.prev(currentPosition); }

    public boolean hasNext()    { return impl.hasNext(currentPosition); }

    public KeyType next()       { return impl.next(currentPosition); }

    public void remove()        {    }

    public int count()          { return impl.count(); }

    public void removeCurrent() {   throw new HGException("Not implemented in JedisStorImpl"); }
}
