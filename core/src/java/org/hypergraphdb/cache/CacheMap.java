package org.hypergraphdb.cache;

/**
 * 
 * <p>
 * A simplified map interface for cache-only purposes. It define two methods
 * for adding elements to the cache with different semantics: 'load' and 'put'.
 * The 'load' method is to be used when data is fetched from permanent storage
 * while the 'put' method is to be used where the data associated with a given
 * key is modified at runtime. This distinction is important for managing the
 * MVCC transactional cache - the 'load' operation is <strong>not</strong> going to 
 * be rolled back in case of an abort!
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public interface CacheMap<K, V>
{
    V get(K key);
    void put(K key, V value);
    void load(K key, V value);
    void remove(K key);
    void clear();
    int size();
}
