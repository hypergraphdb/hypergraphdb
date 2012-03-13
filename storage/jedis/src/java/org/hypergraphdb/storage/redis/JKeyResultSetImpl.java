package org.hypergraphdb.storage.redis;

import org.hypergraphdb.storage.ByteArrayConverter;

import java.util.*;


public final class JKeyResultSetImpl<KeyType> {
    private final ByteArrayConverter<KeyType> keyConverter;
//  private Set<byte[]> resultKeySet;
    private final JedisStore storage;
    private final static Map<Integer, JKeyResultSetImpl> jKRSRegistry = new HashMap<Integer, JKeyResultSetImpl>();
    private final int indexID;
    private final List<byte[]> keys;

    private JKeyResultSetImpl(JedisStore storage, int indexID, Set<byte[]> resultKeySet, ByteArrayConverter<KeyType> keyConverter) {
        this.keyConverter = keyConverter;
//        this.resultKeySet = resultKeySet;
        this.storage = storage;
        this.indexID = indexID;
        this.keys = new ArrayList<byte[]>(resultKeySet);
    }

    public static <KeyType> JKeyResultSetImpl<KeyType> getJKeyResultSetImpl(JedisStore storage,
                                                                            int indexID,
                                                                            Set<byte[]> resultKeySet,
                                                                            ByteArrayConverter<KeyType> keyConverter,
                                                                            boolean useCache) {
        if (!useCache)
            return new JKeyResultSetImpl(storage, indexID, resultKeySet, keyConverter);

        else // use cache
        {
            if (jKRSRegistry.containsKey(indexID))
                return jKRSRegistry.get(indexID);
            else
            {
                JKeyResultSetImpl newJKRS = new JKeyResultSetImpl(storage, indexID, resultKeySet, keyConverter);
                jKRSRegistry.put(indexID, newJKRS);
                return newJKRS;
            }
        }
    }

    public int[] goTo(KeyType value, boolean exactMatch) {  // Todo -- optimize! iterating twice over list! is keySet ordered? Binary search?
        int[] result = {-1, -1};
        int indexOf = keys.indexOf(value);
        if(indexOf != -1)
        {
            result[0] = indexOf; result[1] = 0;
            return result;
        }
        else if(!exactMatch)
        {
            byte[] keyBA = keyConverter.toByteArray(value);

            ArrayList<byte[]> sortedList = new ArrayList<byte[]>(keys);


            ByteArrayComparator bac = new ByteArrayComparator();
            sortedList.add(keyBA);
            Collections.sort(sortedList, bac);

//            int soLiAt = Collections.binarySearch(sortedList, keyBA, bac);  // returns -1, 0, 1, not the index!?

            int soLiAt = sortedList.indexOf(keyBA)+1;
            if(soLiAt <= keys.size())
            {
                int resultAt = keys.indexOf(sortedList.get(soLiAt));
                result[0] = resultAt;
                result[1] = 1;
            }
            return result;
        }
        return result;
    }

    public KeyType current(int[] currentPosition) {
        if (currentPosition[0] == -1 || currentPosition[0] == -2 )
            throw new NoSuchElementException();
        return keyConverter.fromByteArray(keys.get(currentPosition[0]));
    }

    public boolean hasPrev(int[] currentPosition) {
        if(currentPosition[0]==-1)
            return false;
        if(currentPosition[0] == -2 && keys.size()> 0)
            return true;
        if(currentPosition[0]<1)
            return false;
        if(currentPosition[0]-1 >= keys.size())
            return false;
        else
            return true;
    }

    public KeyType prev(int[] currentPosition) {
        if(currentPosition[0] == -1)
            throw new NoSuchElementException();
        if(currentPosition[0]==-2)
            currentPosition[0] = keys.size()-1;
        else
            currentPosition[0] --;
        return current(currentPosition);
    }

    public boolean hasNext(int[] currentPosition) {
        if(currentPosition[0]==-2)
            return false;
        if(currentPosition[0] == -1 && keys.size()> 0)
            return true;
        if(currentPosition[0]+1>= keys.size())       // why +1 ? ok, keys.size 1-based while currentPosition[0] 0-based. But: ">=" AND this is to check for the next!
//        if(currentPosition[0]+1> keys.size())
            return false;
        else
            return true;
    }

    public KeyType next(int[] currentPosition) {
        if(currentPosition[0] == -2)
            throw new NoSuchElementException();
        if(currentPosition[0] == -1) currentPosition[0] = 0;
        else currentPosition[0] ++;
        return current(currentPosition);
    }

    public int count() {    return keys.size(); }

    public void removeCurrent() {      }
}