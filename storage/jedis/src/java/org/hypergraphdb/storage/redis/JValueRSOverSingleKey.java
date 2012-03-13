package org.hypergraphdb.storage.redis;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.CountMe;

public class JValueRSOverSingleKey<ValueType> extends JIndexResultSet<ValueType> implements CountMe {

    private int[] currentPosition = new int[1];
    private JValueRSOverSingleKeyImpl<ValueType> resultSet;
    private int indexID;
    private byte[] key;
    private JedisStore storage;
    private ByteArrayConverter<ValueType> valueConverter;
    private boolean useCache;

    public JValueRSOverSingleKey(int indexID, byte[] key,
                                 JedisStore storage,
                                 ByteArrayConverter<ValueType> valueConverter,
                                 boolean useCache) {
        this.currentPosition[0]= -1;
        this.indexID = indexID;
        this.key = key;
        this.storage = storage;
        this.valueConverter = valueConverter;
        this.useCache = useCache;
        this.resultSet = JValueRSOverSingleKeyImpl.getJedisSingleKeyResultSet(key,
                storage,
                indexID,
                valueConverter,
                useCache);
    }

    public GotoResult goTo(ValueType value, boolean exactMatch)
        {
            return resultSet.goTo(value, exactMatch, currentPosition);
        }

    public void goAfterLast()       {   currentPosition[0] = -2;   }

    
    public void goBeforeFirst()     {   currentPosition[0] = -1;   }

    
    public ValueType current()      {   return resultSet.current(currentPosition);  }

    
    public boolean hasPrev()        {   return resultSet.hasPrev(currentPosition);  }

    
    public ValueType prev()         {   return resultSet.prev(currentPosition);     }

    
    public boolean hasNext()        {   return resultSet.hasNext(currentPosition);  }

    
    public ValueType next()         {   return resultSet.next(currentPosition);     }

    
    public void remove() {}

    
    public void close() {}

    public boolean isOrdered()      {   return true;}

    
    public int count()              {   return resultSet.count(); }

    /**
     * Remove current element. After that cursor becomes invalid, so next(), prev()
     * operations will fail. However, a goTo operation should work.
     */
    
    public void removeCurrent() {   throw new HGException ("Not implemented in JedisStorImpl"); }
}