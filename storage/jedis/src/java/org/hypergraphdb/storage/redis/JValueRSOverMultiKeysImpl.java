package org.hypergraphdb.storage.redis;

import org.hypergraphdb.storage.ByteArrayConverter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.*;

// This class is designed as (not quite) referentially transparent compagnion class of JValueResultSetOverMultipleKeys

//public final class JValueRSOverMultiKeysImpl<KeyType, ValueType> {
public final class JValueRSOverMultiKeysImpl<ValueType> {
    // this result set iterates over values of several keys
//    private final ByteArrayConverter<KeyType> keyConverter;
    private final ByteArrayConverter<ValueType> valueConverter;

    private final JedisStore storage;
    private final int jedisDBiD;
    private final List<byte[]> keySet;
    // Cache
    private static Map<byte[], JValueRSOverMultiKeysImpl> jMKVRSRegistry = new HashMap<byte[], JValueRSOverMultiKeysImpl>();   // hash value of Set<KeyType>?
    //   private static Map<Integer, Integer>  keySetLenghtStorHashMap = new HashMap<Integer, Integer>();   // hash value of Set<KeyType>?

//
    // definition of currentPosition
    // first : position in the first Array of keySet    => key
    // second : position in the entry of key            => value
    // Note: currentPosition is not defined as field by design.
    // special abstract positions:
    // -1, -1 =  beforeFirst
    // -2, -2 afterLast

  //  private JValueRSOverMultiKeysImpl(JedisStorImpl storage, int jedisdbid, Set<byte[]> keySet, ByteArrayConverter<KeyType> keyConverter, ByteArrayConverter<ValueType> valueConverter, boolean downwards) {
   //       this.keyConverter = keyConverter;
  private JValueRSOverMultiKeysImpl(JedisStore storage, int jedisdbid, Set<byte[]> keySet, ByteArrayConverter<ValueType> valueConverter, boolean downwards) {
        this.valueConverter = valueConverter;
        this.storage = storage;
        this.jedisDBiD = jedisdbid;
        this.keySet = new LinkedList<byte[]>(keySet);
        if (downwards) {
            Collections.reverse(this.keySet);
        }
    }

//    public static <KeyType, ValueType> JValueRSOverMultiKeysImpl getJedisMultiKeyValueResultSet(JedisStorImpl stor, int jedisdbid, Set<byte[]> keySet, ByteArrayConverter<KeyType> keyConverter, byte[] keyContext, ByteArrayConverter<ValueType> valueConverter, boolean downwards, boolean useCache) {
//        if (!useCache) return new JValueRSOverMultiKeysImpl(stor, jedisdbid, keySet, keyConverter, valueConverter, downwards);
    public static <ValueType> JValueRSOverMultiKeysImpl getJedisMultiKeyValueResultSet(JedisStore stor, int jedisdbid, Set<byte[]> keySet, byte[] keyContext, ByteArrayConverter<ValueType> valueConverter, boolean downwards, boolean useCache) {
      if (!useCache) return new JValueRSOverMultiKeysImpl(stor, jedisdbid, keySet, valueConverter, downwards);

    else // use cache
        {
            if (jMKVRSRegistry.containsKey(keyContext))
                return jMKVRSRegistry.get(keyContext);
            else {
//                JValueRSOverMultiKeysImpl newjMKVRS = new JValueRSOverMultiKeysImpl(stor, jedisdbid, keySet, keyConverter, valueConverter, downwards);
                JValueRSOverMultiKeysImpl newjMKVRS = new JValueRSOverMultiKeysImpl(stor, jedisdbid, keySet, valueConverter, downwards);
                jMKVRSRegistry.put(keyContext, newjMKVRS);
                return newjMKVRS;
            }
        }
    }


    public ValueType current(int[] currentPosition) {
        byte[] resultset;
        if (currentPosition[0] == -1 || currentPosition[0] == -2 || ((keySet.size()) - currentPosition[0]) < 1)     // TODO -- double check
            throw new NoSuchElementException();
        else { resultset = storage.zrangeAt(jedisDBiD, keySet.get(currentPosition[0]), currentPosition[1]);  }
        if (resultset == null)
            throw new NoSuchElementException();
        else
            return valueConverter.fromByteArray(resultset);
    }

    public void close() {}

    public boolean isOrdered() {  // ToDo -- is ordered?
        return true;
    }

    public boolean hasPrev(int[] currentPosition) {
        if (keySet.isEmpty() || ((currentPosition[0]) < 1) || currentPosition[0] == -1)
            return false;

        boolean result = false;
        Jedis jedis = storage.getReadJedis();
        Pipeline pl = jedis.pipelined();
        pl.select(jedisDBiD);
        Set<Response<Long>> zcardsResp = new HashSet<Response<Long>>();
        for (int i = currentPosition[0]; i > 0; i--)
        {
            if (currentPosition[0]==-2 ) i = keySet.size();
            zcardsResp.add(pl.zcard(keySet.get(i)));
        }
        pl.sync();

        Long temp = null;
        for (Response<Long> rl : zcardsResp)
        {
            temp = rl.get();
            if (temp != null)
                if (temp>0)
                    result=true;
        }
        storage.returnReadJedis(jedis);
        return result;
    }


    public ValueType prev(int[] currentPosition) {

        if(currentPosition[0] == -1)
            throw new NoSuchElementException();
        if(currentPosition[0] == -2)
            currentPosition[0] = keySet.size();

        byte[] prev = null;
        if (currentPosition[1] >0 ) prev = storage.zrangeAt(jedisDBiD, keySet.get(currentPosition[0]), currentPosition[1] - 1);
        if (prev != null) currentPosition[1] = currentPosition[1] - 1;
        if (prev == null && currentPosition[0]>0)   // if this didn't yield anything, and if this isn't already the first key, get last element from previous key
            {
                prev = storage.zrangeAt(jedisDBiD, keySet.get(currentPosition[0] - 1), -1);    // (the last -1 refers to "last element")
                if(prev != null) {
                    currentPosition[0] = currentPosition[0] - 1;
                    currentPosition[1] = (storage.zcard(jedisDBiD, keySet.get(currentPosition[0]))).intValue()-1;       // zcard result is 1-based, currentPosition is an Array! Checking with -1
                }
            }
        if (prev == null)
            throw new NoSuchElementException();

        return valueConverter.fromByteArray(prev);
    }

    public boolean hasNext(int[] currentPosition) {
        if (keySet.isEmpty() || ((keySet.size() - currentPosition[0]) <= 1) || currentPosition[0] == -2)
            return false;

        boolean result = false;

        Jedis jedis = storage.getReadJedis();
        Pipeline pl = jedis.pipelined();
        pl.select(jedisDBiD);

//        System.out.println("keyset size is :" + keySet.size() + " . currentPosition: " + currentPosition[0] + "keySet.get(0)" + keySet.get(0));
        Set<Response<Long>> zcardsResp = new HashSet<Response<Long>>(keySet.size());
        if(currentPosition[0] >=-1)
        {
            int i = currentPosition[0];
            while( i < keySet.size())  // keySet is a ArrayList...
            {
                if(i==-1) i=0;
                zcardsResp.add(pl.zcard(keySet.get(i)));           // ... , which is zero-based.
                i++;
            }
        }
        pl.sync();

        Long temp = null;
        for (Response<Long> rl : zcardsResp)
        {
            temp = rl.get();
            if (temp != null)
                if (temp>0)
                {
                    result=true;
                    break;
                }
        }

        storage.returnReadJedis(jedis);
        return result;
    }

    public ValueType next(int[] currentPosition) {
//        int key = currentPosition[0];
        if (currentPosition[0] < -1)
            throw new NoSuchElementException();
        if(currentPosition[0] == -1)
            currentPosition[0] = 0;

        byte[] next = storage.zrangeAt(jedisDBiD, keySet.get(currentPosition[0]), currentPosition[1] + 1);
        if (next!=null)
            currentPosition[1] = currentPosition[1] +1;

        else   // if this didn't yield anything, get first element ("0") from next key
        {
            try
            {
                next = storage.zrangeAt(jedisDBiD, keySet.get(currentPosition[0]+1), 0);
                // in case there is no next key, and we get null, don't touch the cursor.
                if (next != null)
                {
                    currentPosition[0] = currentPosition[0] + 1;
                    currentPosition[1] = 0;
                }
            }
            catch(Exception ex) {}
            if (next == null)
                throw new NoSuchElementException();
        }
        return valueConverter.fromByteArray(next);
    }

    public void remove() { }

    public int[] goTo(ValueType value, boolean exactMatch) {
        if (exactMatch)
            return goToExact(value);
        else
            return goToNotExact(value);
    }

    public int[] goToExact(ValueType value) {
        int[] result = {-1, -1, -1};  // position 0 and 1 correspond to currentPosition, Position 2 to GoTo Result, where 0 found, 1 close, -1 nothing
        byte[] valueBA = valueConverter.toByteArray(value);
        List<Response<Long>> zrankRes = new ArrayList<Response<Long>>(keySet.size());
        Jedis jedis = storage.getReadJedis();
        Pipeline pl = jedis.pipelined();
        pl.select(jedisDBiD);
        for (int i = 0; i < keySet.size(); i++)
            zrankRes.add(pl.zrank(keySet.get(i), valueBA));   // zrank is 0 based!
        pl.sync();

        List<Long> zrankList = new ArrayList<Long>(keySet.size());
        for(Response<Long> lr : zrankRes)
            zrankList.add(lr.get());

        storage.returnReadJedis(jedis);

        for(int i = 0; i < zrankList.size(); i++)
        {  if (zrankList.get(i) != null)
                if (zrankList.get(i).intValue() >= 0)
                {
                    result[0] = i;
                    result[1] = zrankList.get(i).intValue();
                    result[2] = 0;
                    return result;
                }
        }
        return result;
    }


    public int[] goToNotExact(ValueType value) {

        int[] result = goToExact(value);
        if(result[2]== 0)
                return result;
        if (result[2]==-1)
        {
            // make a 2D- array and accomanying list
            int[][] table = new int[keySet.size()][2];
            ArrayList<byte[]> byteAList = new ArrayList<byte[]>(keySet.size());

            // populate both..
            for (int i = 0; i < keySet.size(); i++) {                // TODO - this is inefficient. Maybe it should use 1 transaction, safe Responses in a list, and zrangeAt in byteAList.add through the response List in a separte Pipeline
                // ... each row of table with nubmer of key and insertion point of value in it.
                table[i][2] = i;
                table[i][1] = storage.getRankOrInsertionPoint(jedisDBiD, keySet.get(i), valueConverter.toByteArray(value));
                // add the entry next insertion point, if insertion point would be last, this would be null
                byteAList.add((storage.zrangeAt(jedisDBiD, keySet.get(i), table[i][1])));
                // the value at Insertion point, is the next bigger one.

            }

            // COMPARATOR
            ArrayList<byte[]> sortedBAList = new ArrayList<byte[]>(byteAList);
            Collections.sort(sortedBAList, new ByteArrayComparator());   // comparator from apache hbase

            // CONTINUATION OF METHOD
            int smallestIndex = byteAList.indexOf(sortedBAList.get(1));   // this should correspond to the number of the key in keyset with the smallest greater than
            result[2] = 1;
            result[1] = storage.zrank(jedisDBiD, keySet.get(smallestIndex), valueConverter.toByteArray(value)).intValue();
            result[0] = smallestIndex;

            // 3: key
            // 2: the insertion point of value for each key's sose.
            // 1: the next bigger element after the respective insertion point.

            // then sort on column 1 for the smallest next-bigger element, then retrieve it's key and rank
            //            --> http://tips4java.wordpress.com/2008/10/16/column-comparator/

        }
        return result;
    }

    public int count() {
            int count = 0;
            for(byte[] key: keySet)
                    count += storage.zcard(jedisDBiD, key).intValue();
            return count;
    }
}