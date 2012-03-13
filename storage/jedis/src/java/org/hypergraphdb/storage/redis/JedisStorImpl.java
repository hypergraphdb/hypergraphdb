package org.hypergraphdb.storage.redis;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.hypergraphdb.*;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.HGConverter;
import org.hypergraphdb.transaction.*;
import org.hypergraphdb.transaction.JedisStorTransaction;
import redis.clients.jedis.*;
import redis.clients.util.Pool;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JedisStorImpl implements JedisStore {
//    public static final byte[] KEYS = "keySet".getBytes();

    public static final byte[] INDICES = "indices".getBytes();
    private HGStore store;
    private HGHandleFactory handleFactory;
    private HashMap<String, HGIndex<?,?>> openIndices = new HashMap<String, HGIndex<?,?>>();
    private ReentrantReadWriteLock indicesLock = new ReentrantReadWriteLock();
    private int handleSize;
//    private byte[] tag = {0x0F, 0x0E, 0x0F, 0x0E };

        // Master
    private Pool<Jedis> writePool = null;
    private int masterTimeout = 2000;

    @Override
    public Pool<Jedis> getWritePool() { return writePool; }
    private String masterIP = "127.0.0.1";
    private int masterPort = 6378;
    private String masterPasswort = "";
    private JedisPoolConfig masterPoolConfig = new JedisPoolConfig();

    // Slaves
    private Set <JedisShardInfo> slaves = new HashSet<JedisShardInfo>();           // Add the possibility to use >only< the provided, without publishing them to the Master Slave list
    private List <JedisShardInfo> offlineSlaves;
    private Pool<Jedis> readPool;


    private int redundancyFactor = 0;                   // if set to 0, means only master is used!
    private boolean syncRedundancy = true;
    private boolean useCache = false;

    public JedisStorImpl() { }


    // Redis Databases:
    // 0: configuration: List:SlaveSockets, Sose for each slaveSocket:Timestamp:ResponseTime, slaveSocket-TimeStamp:Status
    // 1: LinkDB   - this is were the links are saved. IMPORTANT: in BerkeleyDB implementation this was called "datadb"!
    // 2. DataDB   - this is were value atoms are saved. IMPORTANT: in BerkeleyDB implementation this was called primitiveDB!
    // 3. IncidenceDB
    // 4 - n Indices
    // there should be <<1000 indices : https://groups.google.com/d/topic/redis-db/yjU4XCkZ6HA/discussion

    
    // Due to restricted transaction support in Redis (no intra-transactional dependencies, no rollback, no true isolation), transactions are disabled.
    private JedisStorTransaction txn()       // direct BinaryTransaction??!?
    {
        HGTransaction tx = store.getTransactionManager().getContext().getCurrent();
        if (tx == null || tx.getStorageTransaction() instanceof VanillaTransaction)
            return JedisStorTransaction.nullTransaction();
        else
            return (JedisStorTransaction) tx.getStorageTransaction();
    }


    public void startup(HGStore store, HGConfiguration config)
    {

        String location = store.getDatabaseLocation();

        
        masterPoolConfig.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_GROW);
        if(masterPoolConfig.getMinIdle() < 3) masterPoolConfig.setMinIdle(3);

        //masterPoolConfig.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
        this.store = store;
        this.handleFactory = config.getHandleFactory();
        this.handleSize = this.handleFactory.nullHandle().toByteArray().length;


        if (location != null && location != "")
            masterIP = location;
        else
            masterIP = "localhost";

        // check if there are local redis-servers running, other than master.  
        try {
            for (int i = 6375; i<6385;i++){
                if(i != masterPort && checkForRunningRedis("localhost", i))
                {   System.out.println("Found a locally running slave at port " + i + ". Adding to slave Set.");
                    slaves.add(new JedisShardInfo(java.net.InetAddress.getLocalHost().getHostAddress(), i));
                }
            }
            } 
        catch (UnknownHostException e) { e.printStackTrace(); }
        

        // Register those found so far on redis DB 0 for others to find.

        Jedis j = null;
        if(!slaves.isEmpty() && slaves.size() >0)
        {
         j = new Jedis(masterIP, masterPort);
         j.select(0);
            for(JedisShardInfo jsi : slaves)
            {   System.out.println("Registering local slave " + jsi.getHost() + "at master DB 0");
                j.hset("slaves", jsi.getHost(), String.valueOf(jsi.getPort()));
            }
            j.disconnect();
        }

/*
        // retrieve those of others.
        try { Thread.sleep(3000);  } catch (InterruptedException e) { } //waiting a bit for others to publish theirs, remove for non-testing scenarios

        j = new Jedis(masterIP, masterPort);
        j.select(0);
        Map<String, String> slaveMap = j.hgetAll("slaves") ;
        j.disconnect();
        if (!slaveMap.isEmpty())
            for(Map.Entry<String, String> mss : slaveMap.entrySet())
            {
                j = new Jedis(masterIP, masterPort);
                if(j != null && j.ping().equals("PONG"))
                {   System.out.println("Found external slave at " + mss.getKey() + " at Port " + mss.getValue() + ". \nRegistering new slave.");
                    slaves.add(new JedisShardInfo(mss.getKey(),Integer.valueOf(mss.getValue())));
                }
            }

*/

        // initialize connection to Jedis Master and Read Pools
        writePool = new JedisPool(masterPoolConfig, masterIP, masterPort, masterTimeout, masterPasswort);
        if (redundancyFactor>0)
//            this.readPool = new RoundRobinPool(new GenericObjectPool.Config(), masterIP, masterPort, masterPasswort, slaves);
              this.readPool = new RoundRobinPool(masterPoolConfig, masterIP, masterPort, masterPasswort, new ArrayList<JedisShardInfo>(slaves));
        else
            this.readPool = writePool;

        checkMaster();

        if (redundancyFactor>0)
            checkSlaves();

        System.out.print("in storImpl:startup().  RedundancyFactor is set to " + redundancyFactor + ". ReadPool equals writepool? " + readPool.equals(writePool));

    }

    private boolean checkForRunningRedis(String host, int port){
        boolean result = false;
        Jedis j =  null;
        try {
             j = new Jedis(host, port);
            if(j.ping().equals("PONG"))
                result =  true;
            else
                result = false;
        } 
        catch (Exception e) {}
        finally { j.disconnect(); }
        return result;
    }


    private void checkMaster(){
        Jedis writeJedis =null;
        try {
            writeJedis = writePool.getResource();
            Thread.sleep(50);
            if(!(writeJedis.ping().equals("PONG")))
                    throw new HGException("Master didn't answer to ping!");
        }
        catch (Exception ex) {
            if(!isReachable(masterIP))
                throw new HGException("failed to startup master. Master didn't answer to Jedis ping. Possible a network error, Master's IP " + masterIP + " is not reachable");
            else
                throw new HGException("failed to startup master. Master didn't answer to Jedis ping. Check redis server, Master's IP " + masterIP + " is reachable.");
        }
        finally { try{ writePool.returnResource(writeJedis); } catch(Exception ex){}}

    }

    private void checkSlaves(){
            Jedis j = null;
            try
            {
                for(JedisShardInfo jsi: slaves)
                {
                    try
                    {
                        j = jsi.createResource();
                        if(j == null || !(j.ping().equals("PONG")))
                        {
                            if(offlineSlaves==null)
                                offlineSlaves = new ArrayList<JedisShardInfo>();
                            offlineSlaves.add(jsi);
                            slaves.remove(jsi);
                        }

                    }
                    catch (Exception ex) {}
                    finally{ try{j.disconnect();} catch (Exception ex) {}}
                }

                if(offlineSlaves!=null)
                    if(offlineSlaves.size()<slaves.size())
                        {
                            System.out.println("WARNING: failed to start up some slaves. Following slaves didn't answer to jedis Ping: " );
                            for (JedisShardInfo jsi: offlineSlaves)
                                System.out.println(jsi.toString());
                        }
                    if(offlineSlaves.size()==slaves.size())
                    {
                        System.out.println("WARNING: failed to start up any slaves. No answer to jedis ping. Switching to master-only mode. ");
                        this.redundancyFactor = 0;
                    }
            }
            catch(Exception ex){}
        }


    public void shutdown()
    {
        for (Iterator<HGIndex<?,?>> i = openIndices.values().iterator(); i.hasNext(); )
            try
            {
                i.next().close();
            }
            catch (Throwable t)
            {
                t.printStackTrace();
            }

        try
        {
            writePool.destroy();

        //    Jedis j = new Jedis(masterIP, masterPort);
        //    j.auth(masterPasswort);
            // explicitely save: may cause: "ERR Background save already in progress"
//            j.save();
//            j.shutdown();
          //  j.disconnect();
        }
        catch (redis.clients.jedis.exceptions.JedisDataException jde) { }
        catch (Exception ex) {System.out.println("Failed to shutdown slaves. "+ ex.toString());}

        if(redundancyFactor>0 || slaves !=null)
        try{
            {
                for(JedisShardInfo jsi: slaves)
                {
                    Jedis js = jsi.createResource();
                    js.save();
  //                  js.shutdown();
                    js.disconnect();
                }
            }
        }
        catch (redis.clients.jedis.exceptions.JedisDataException jde) { }
        catch (Exception ex) {System.out.println("Failed to shutdown slaves. "+ ex.toString());}
    }

    public void removeLink(HGPersistentHandle handle)
    {
        Jedis j = writePool.getResource();
        if (handle == null)
            throw new NullPointerException("HGStore.remove called with a null handle.");

        try
        {
            j.watch(handle.toByteArray());
            BinaryTransaction currentBT = j.multi();   //txn().getBinaryTransaction();
            currentBT.select(1);
            currentBT.del(handle.toByteArray());
            currentBT.exec();
        }
        catch (Exception ex) {throw new HGException("Failed to remove value with handle " + handle + ": " + ex.toString(), ex);}
        finally { writePool.returnResource(j); }
    }

    public HGPersistentHandle store(HGPersistentHandle handle, byte[] data)
    {
        Jedis j = writePool.getResource();
        if (handle == null)
            throw new NullPointerException("HGStore.store called with a null handle.");

        Response<Long> succeed;
        try
        {
            j.watch(handle.toByteArray());
            BinaryTransaction currentBT = j.multi();
            currentBT.select(2);
            currentBT.set(handle.toByteArray(), data);
            currentBT.exec();
        }

        catch (Exception ex) {throw new HGException("Failed to store raw byte []: " + ex.toString(), ex);}
        finally { writePool.returnResource(j); }
        return handle;
    }

    public HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle[] link)
    {
        Jedis j = writePool.getResource();
        if (handle == null)
            throw new HGException("StorageImpl.getLink() is being called with null handle.");

        byte[] blink = HGConverter.convertHandleArrayToByteArray(link, handleSize);
        try
        {
            BinaryTransaction currentBT = j.multi();
            currentBT.select(1);
            currentBT.set(handle.toByteArray(), blink);
            currentBT.exec();
        }
        catch (Exception ex)  {throw new HGException("Failed to store link: " + ex.toString(), ex); }
        finally { writePool.returnResource(j); }
        return handle;
    }

    public void addIncidenceLink(HGPersistentHandle handle, HGPersistentHandle newLink)
    {
        Jedis j = writePool.getResource();
        try
        {
            BinaryTransaction currentBT = j.multi();
            currentBT.select(3);
            currentBT.zadd(handle.toByteArray(), 0, newLink.toByteArray());  //TODO - lexicographic order or insertion order?
            //currentBT.lpush(handle.toByteArray(), newLink.toByteArray());  // list would be good for insertion order, but then there could be duplicates.
            currentBT.exec();
        }
        catch (Exception ex) {throw new HGException("Failed to update incidence set for handle " + handle + ": " + ex.toString(), ex);}
        finally { writePool.returnResource(j); }
    }

    public boolean containsLink(HGPersistentHandle handle) { return containsLink(handle, redundancyFactor);}
    public boolean containsLink(HGPersistentHandle handle, int toTryCount)
    {
        boolean result = false;
        Jedis jedis = null;

        try
        {
            if(toTryCount>0)
                jedis = (Jedis) readPool.getResource();
            else
                jedis = (Jedis) writePool.getResource();

            if(toTryCount<redundancyFactor)  assert(jedis.ping().equals("PONG"));      // this means we are in some redundancy scenario

            Pipeline p = jedis.pipelined();
            p.select(1);
            Response<Boolean> tempR = p.exists(handle.toByteArray());
            p.sync();

            try { result = tempR.get();} catch (Exception e) {}


        }
        catch (Exception ex)
        {
            if(toTryCount>0)
                result= containsLink(handle, toTryCount-1);
            else
                throw new HGException("StorImpl containsLink() for handle " + handle + " failed. " + ex.getStackTrace().toString());
        }
        finally
        {
            if(toTryCount>0)
                readPool.returnResource(jedis);
            else
                writePool.returnResource(jedis);
        }

        if (result == false && toTryCount >0 && syncRedundancy)
        {
            result = containsLink(handle, toTryCount-1);
        }

        return result;
    }

    public byte[] getData(HGPersistentHandle handle) {return getData(handle, redundancyFactor);}
    public byte[] getData(HGPersistentHandle handle, int toTryCount)
    {
        byte[] result = null;
        Jedis jedis = null;
        try
        {
            if(toTryCount>0)
                jedis = (Jedis) readPool.getResource();
            else
                jedis = (Jedis) writePool.getResource();

            if(toTryCount<redundancyFactor)  assert(jedis.ping().equals("PONG"));      // this means we are in some redundancy scenario

            Pipeline p = jedis.pipelined();
            p.select(2);
            Response<byte[]> tempR = p.get(handle.toByteArray());
            p.sync();

            try { result = tempR.get();} catch (Exception e) {}
        }

        catch (Exception ex)
        {
            if(toTryCount>0)
                result= getData(handle, toTryCount-1);
            else
                throw new HGException("StorImpl getData for handle " + handle +  " failed." + ex.toString(), ex);
        }
          finally
        {
            if(toTryCount>0)
                readPool.returnResource(jedis);
            else
                writePool.returnResource(jedis);
        }
        
        if (result == null && toTryCount >0 && syncRedundancy)   {
            result = getData(handle, toTryCount-1); }
        
        return result;
    }


    public HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(HGPersistentHandle handle) {
        if (handle == null)
            throw new NullPointerException("HGStore.getIncidenceResultSet called with a null handle.");
        HGRandomAccessResult<HGPersistentHandle> result = null;

        Set<byte[]> temp = zrange(3, handle.toByteArray(), 0, -1, redundancyFactor);

            //Set<byte[]> temp = new HashSet<byte[]>(jedis.lrange(handle.toByteArray(), 0, -1));
        if (temp == null)
            return (HGRandomAccessResult<HGPersistentHandle>)HGSearchResult.EMPTY;
        else
        {
                
                byte[] keyContext = HGConverter.concat("getIncidenceResultSet".getBytes(), handle.toByteArray());
                result = new JValueRSOverSingleKey<HGPersistentHandle>(3, handle.toByteArray(), this, BAtoHandle.getInstance(handleFactory), useCache);
                //result = new JValueRSOverMultiKeys<HGPersistentHandle>(this, 3, temp, BAtoHandle.getInstance(handleFactory), keyContext, true, useCache);
        }

        return result;
    }

    public long getIncidenceSetCardinality(HGPersistentHandle handle) {
        if (handle == null)
            throw new NullPointerException("HGStore.getIncidenceSetCardinality called with a null handle.");
        long result;
       result = zcard(3, handle.toByteArray(), redundancyFactor);
       return result;
    }

    public HGPersistentHandle[] getLink(HGPersistentHandle handle) {return getLink(handle, redundancyFactor);}
    public HGPersistentHandle[] getLink(HGPersistentHandle handle, int toTryCount)
    {
        HGPersistentHandle[] result = null;
        Jedis jedis = null;
        try
        {
            if(toTryCount>0)    jedis = (Jedis) readPool.getResource();
            else                jedis = (Jedis) writePool.getResource();

            if(toTryCount<redundancyFactor)  assert(jedis.ping().equals("PONG"));      // this means we are in some redundancy scenario

            jedis.select(1);
            byte[] temp = jedis.get(handle.toByteArray());
            result = HGConverter.convertByteArrayToHandleArray(temp, handleFactory);
                }
        catch (Exception ex)
        {
            if(toTryCount>0)    result= getLink(handle, toTryCount-1);
            else                throw new HGException("Failed to retrieve link with handle " + handle, ex);
        }
        finally
        {
            if(toTryCount>0)    readPool.returnResource(jedis);
            else                writePool.returnResource(jedis);
        }

        if (result == null && toTryCount > 0 && syncRedundancy){
            result = getLink(handle, toTryCount-1);
        }

        return result;
    }

    public HGTransactionFactory getTransactionFactory()
    {
        return new HGTransactionFactory()
        {
            public HGStorageTransaction createTransaction(HGTransactionContext context, HGTransactionConfig config, HGTransaction parent)
            {
                HGStorageTransaction result;
                BinaryTransaction binTransa = null;

                Jedis writeJedis = writePool.getResource();
                result = new JedisStorTransaction(writeJedis, binTransa);
                return result;
            }
            @Override
            public boolean canRetryAfter(Throwable t) { return false; }   // Todo  -- confirm JedisStorImpl-Pseudo-Transaction alway "canRetryAfter"
        };
    }

    public void removeData(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.remove called with a null handle.");
        Jedis j = writePool.getResource();
        try
        {
            BinaryTransaction currentBT = j.multi();
            currentBT.select(2);
            currentBT.del(handle.toByteArray());
            currentBT.exec();
        }
        catch (Exception ex) { throw new HGException("Failed to remove value with handle" + handle + ": " + ex.toString(), ex); }
        finally { writePool.returnResource(j);}
    }

    public void removeIncidenceLink(HGPersistentHandle handle, HGPersistentHandle oldLink)  {
        Jedis j = writePool.getResource();
        try
        {
            BinaryTransaction currentBT = j.multi();
            currentBT.select(3);
            currentBT.zrem(handle.toByteArray(), oldLink.toByteArray());
            //currentBT.lrem(handle.toByteArray(),0, oldLink.toByteArray());
            currentBT.exec();
        }
        catch (Exception ex) { throw new HGException("Failed to update incidence set for handle " + handle + ": " + ex.toString(), ex); }
        finally { writePool.returnResource(j);}
    }

    public void removeIncidenceSet(HGPersistentHandle handle)  {
        Jedis j = writePool.getResource();
        try
        {
            BinaryTransaction currentBT = j.multi();
            currentBT.select(3);
            currentBT.del(handle.toByteArray());
            currentBT.exec();
        }
        catch (Exception ex) { throw new HGException("Failed to remove incidence set of handle " + handle + ": " + ex.toString(), ex);  }
        finally { writePool.returnResource(j);}
    }

    // ------------------------------------------------------------------------
    // INDEXING
    // ------------------------------------------------------------------------

    boolean checkIndexExisting(String name)
    {
        if (openIndices.get(name) != null)
            return true;
        else
            return false;

    }

    @Override
    public int lookupIndexID(String DBname)     // TODO - needs testing!!
    {
        byte[] temp = null;
        Jedis j = null;
        try
        {
            j = getWritePool().getResource();
            j.select(0);
            temp= j.hget(INDICES, DBname.getBytes());
        }
        catch (Exception e){}
        finally { getWritePool().returnResource(j); }

        Integer tempVal = HGConverter.byteArrayToInt(temp);
        if(tempVal == null)
               return -1;
        else
                return tempVal;
    }

    @Override
    public Integer lookupIndexIdElseCreate(String dbName)     // TODO - needs checking!!
    {
        byte[] temp = null;
        byte[] dbNameBA = dbName.getBytes();
        int result = 0;
        Jedis j = null;

        try
        {
            j = getWritePool().getResource();
            j.select(0);
            temp= j.hget(INDICES, dbNameBA);

            if (temp != null)
                result = HGConverter.byteArrayToInt(temp);

            if(result >= 4)
                return result;

            if(result < 4 && result > 0)
                if(j.hexists(INDICES, dbNameBA))   // this should not be the case, since database 0-3 are reserved for datadb, linkdb and incidencedb.
                    throw new HGException("lookupIndexElseCreate, DB " + dbName + " is listed in indices, but was associated to an invalid database. The value in the field is: " + HGConverter.byteArrayToInt(j.hget(INDICES, dbName.getBytes())));
                else
                    throw new HGException("lookupIndexElseCreate, DB " + dbName + " was associated to an invalid database. The value in the field is: " + HGConverter.byteArrayToInt(j.hget(INDICES, dbName.getBytes())));
// if index name not registered, create a new one.
            if(temp == null)
            {
                int newIndex = j.hlen(INDICES).intValue() + 4;   // the new database ID corresponds to the number of entries + 4, dataDB, linkDB, incidenceDB and db0

                j.select(newIndex);
                Transaction t = j.multi();
                t.select(0);
                t.hset(INDICES,dbNameBA,HGConverter.intToByteArray(newIndex));
                t.exec();
            }

            else   // should never be negative
                    throw new HGException("lookupIndexElseCreate, DB " + dbName + " is listed in indices, but was associated to an invalid database. The value in the field is: " + HGConverter.byteArrayToInt(j.hget(INDICES, dbName.getBytes())));
        }
        finally { returnWriteJedis(j); }
        return result;
    }


    @SuppressWarnings("unchecked")
    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(
            String name,
            ByteArrayConverter<KeyType> keyConverter,
            ByteArrayConverter<ValueType> valueConverter,
            Comparator<?> comparator,
            boolean isBidirectional,
            boolean createIfNecessary)
    {

        // TODO -- no chance to integrate lookupIndexIDelseCreate with getIndex() at least a little bit?!

        indicesLock.readLock().lock();
        try
        {
            HGIndex<KeyType, ValueType> idx = (HGIndex<KeyType, ValueType>)openIndices.get(name);
            if (idx != null)
                return idx;
            if (!checkIndexExisting(name) && !createIfNecessary)
                return null;
        }
        finally {indicesLock.readLock().unlock(); }

        indicesLock.writeLock().lock();
        try
        {
            HGIndex<KeyType, ValueType> idx = (HGIndex<KeyType, ValueType>)openIndices.get(name);
            if (idx != null)
                return idx;
            if (!checkIndexExisting(name) && !createIfNecessary)
                return null;

            JedisIndex<KeyType, ValueType> result = null;

            if (isBidirectional)
                result =  new JedisBidirectionalIndex<KeyType, ValueType>(name,
                        this,
                        store.getTransactionManager(),
                        keyConverter,
                        valueConverter,
                        comparator);
            else
                result = new JedisIndex<KeyType, ValueType>(name,
                        this,
                        store.getTransactionManager(),
                        keyConverter,
                        valueConverter,
                        comparator);
            result.open();
            openIndices.put(name, result);
            return result;
        }
        finally
        {
            indicesLock.writeLock().unlock();
        }
    }


    @SuppressWarnings("unchecked")
    public void removeIndex(String name)            // TODO -- double check. dangerous..
    {
        indicesLock.writeLock().lock();
        try
        {
            HGIndex idx = openIndices.get(name);
            if (idx != null)
            {
                idx.close();
                openIndices.remove(name);
            }
        }
        finally
        {
            indicesLock.writeLock().unlock();
        }
        Jedis writeJedis = null;
        int dbID = lookupIndexID("hgstore_idx_" +name);

        try
        {   writeJedis  = writePool.getResource();
            writeJedis.select(0);
            writeJedis.watch(INDICES);
            BinaryTransaction bt = writeJedis.multi();
            bt.hdel(INDICES, name.getBytes());
            bt.select(dbID);
            bt.flushdb();
            bt.exec();
        }
        catch (Exception e)
        {
            throw new HGException(e);
        }
        finally {writePool.returnResource(writeJedis);}
    }



    public Object getConfiguration() {
        return null;
    }

    //GETTERS
    public HGHandleFactory getHandleFactory() {
        return handleFactory;
    }

    @Override
    public Pool<Jedis> getReadPool() {
        return readPool;
    }


    @Override
    public Jedis getReadJedis() {
        if(redundancyFactor>0)
            return readPool.getResource();
        else
            return writePool.getResource();
    }

    @Override
    public void returnReadJedis(Jedis j) {
        try
        {
            if (redundancyFactor>0)
                readPool.returnResource(j);
            else
                writePool.returnResource(j);
        }
        catch (Exception ex) { writePool.returnBrokenResource(j); readPool.returnBrokenResource(j);}  // TODO -- insert right exception here
    }

    @Override
    public void returnWriteJedis(Jedis j) {
        writePool.returnResource(j);
    }

    @Override
    public void setUseCache(boolean useCache) { this.useCache = useCache; }

    // Jedis Utility methods.
    @Override
    public Set<byte[]> zrange(int jedisDBiD, byte[] key, int i, int i1) {return zrange(jedisDBiD, key, i, i1, redundancyFactor);}
    public Set<byte[]> zrange(int jedisDBiD, byte[] key, int i, int i1, int toTryCount) {
//        Set<byte[]> result = new TreeSet<byte[]>(new ByteArrayComparator());

        Set<byte[]> result = null;
        Jedis jedis = null;
        try{
            if(toTryCount>0) jedis = readPool.getResource();
            else jedis = writePool.getResource();

            if(toTryCount<redundancyFactor)  assert(jedis.ping().equals("PONG"));      // this means we are in some redundancy scenario

            Pipeline p = jedis.pipelined();
            p.select(jedisDBiD);
            Response<Set<byte[]>> tempR = p.zrange(key, i, i1);  // WARNING - implied bug fix in Jedis Pipeline class! previously returned Set<String>
            p.sync();
            try { result = tempR.get();} catch (Exception e) {}

            
//            result.addAll(temp);
        }
        catch (Exception ex)
        {
            if(toTryCount==0)
                throw new HGException("zrange failed: " + ex.getStackTrace());
            else
                result = zrange(jedisDBiD, key, i, i1, toTryCount-1);
        }
        finally
        {
            if(toTryCount>0)
                readPool.returnResource(jedis);
            else
                writePool.returnResource(jedis);
        }

        if(result == null && toTryCount >0 && syncRedundancy) {
            result = zrange(jedisDBiD, key, i, i1, toTryCount-1);
        }
        
        return result;
    }

    @Override
    public byte[] zrangeAt(int jedisDBiD, byte[] key, int i) {
        Set<byte[]> result  = zrange(jedisDBiD, key, i, i);
        if(!result.isEmpty()) return result.iterator().next();
        else return null;
    }

    @Override
    public Long zcard(int jedisDBiD, byte[] key) {return zcard(jedisDBiD, key, redundancyFactor);}
    public Long zcard(int jedisDBiD, byte[] key, int toTryCount)
    {
        Long result =null;
        Jedis jedis = null;
        try{
            if(toTryCount>0)
                jedis = getReadJedis();
            else
                jedis = writePool.getResource();

            if(toTryCount<redundancyFactor)  assert(jedis.ping().equals("PONG"));      // this means we are in some redundancy scenario

            Pipeline p = jedis.pipelined();
            p.select(jedisDBiD);
            Response<Long> tempR = p.zcard(key);
            p.sync();
            try { result = tempR.get();} catch (Exception e) {}

           
        }
         catch (Exception ex)
        {
            if(toTryCount!=0)
                result = zcard(jedisDBiD, key, toTryCount-1);
            else
                throw new HGException("zcard failed: " + ex.getStackTrace());

        }
         finally
        {
            if(toTryCount>0)
                readPool.returnResource(jedis);
            else
                writePool.returnResource(jedis);
        }

        if(result ==null && toTryCount >0 && syncRedundancy) {result = zcard(jedisDBiD, key, toTryCount-1); }
        
        return result;
    }

    @Override
    public Long zrank(int jedisDBiD, byte[] key, byte[] value) {return zrank(jedisDBiD, key,value, redundancyFactor);}
    public Long zrank(int jedisDBiD, byte[] key, byte[] value, int toTryCount)
     {
         Long result = null;
         Jedis jedis = null;
         try{
             if(toTryCount>0)
                 jedis = getReadJedis();
             else
                 jedis = writePool.getResource();

             if(toTryCount<redundancyFactor)  assert(jedis.ping().equals("PONG"));      // this means we are in some redundancy scenario

             Pipeline p = jedis.pipelined();
             p.select(jedisDBiD);
             Response<Long> tempR = p.zrank(key, value);
             p.sync();
             try { result = tempR.get();} catch (Exception e) {}
         }
          catch (Exception ex)
         {
            if(toTryCount>0)
                 result = zcard(jedisDBiD, key, toTryCount-1);
            else
                throw new HGException("zrank failed: " + ex.getStackTrace());
         }
          finally
         {
             if(toTryCount>0)
                 readPool.returnResource(jedis);
             else
                 writePool.returnResource(jedis);
         }

         if(result ==null && toTryCount >0 && syncRedundancy) { result = zcard(jedisDBiD, key, toTryCount-1); }

         return result;
     }

    @Override
    public Set<byte[]> keys(int jedisDbId, byte[] arg) { return keys(jedisDbId,arg, redundancyFactor);}
    public Set<byte[]> keys(int jedisDbId, byte[] arg, int toTryCount) {
 //       Set<byte[]> result = new TreeSet<byte[]>(new ByteArrayComparator());
        Set<byte[]> result = null;
        Jedis jedis=null;
        try{
            if(toTryCount>0)
                jedis = readPool.getResource();
            else
                jedis = writePool.getResource();

            if(toTryCount < redundancyFactor)  assert(jedis.ping().equals("PONG"));      // this means we are in some redundancy scenario

            jedis.select(jedisDbId);
            result = jedis.keys(arg);
            
            /*  // suspicion there is some bug in the Pipeline version of keys? i.e. in the builder
            Pipeline p = jedis.pipelined();
            p.select(jedisDbId);
            Response<Set<byte[]>> tempR = p.keys(arg);
            p.sync();
            try { result = tempR.get();} catch (Exception e) {}
            */

        }
        catch (Exception ex) {
            if(toTryCount>0)
                result = keys(jedisDbId, arg, toTryCount -1);
            else{
                ex.printStackTrace();
                throw new HGException("keySet failed: ");}
        }
        finally
        {
            if(toTryCount>0)
                readPool.returnResource(jedis);
            else
                writePool.returnResource(jedis);
        }

        if(result ==null && toTryCount >0 && syncRedundancy) { result = keys(jedisDbId, arg, toTryCount-1); }

        return result;
    }


    @Override
    public int getRankOrInsertionPoint(int dbId, byte[] key, byte[] value) {
        int result;
        Long temp = zrank(dbId, key, value);
        
        if(temp != null)
               return temp.intValue();
        else
            return getInsertionPoint(dbId, key, value, redundancyFactor);
    }

    public int getInsertionPoint(int dbId, byte[] key, byte[] value, int toTryCount) 
    {
        List<byte[]> origZrange = new ArrayList<byte[]> (zrange(dbId,key,0, -1, redundancyFactor));
        origZrange.add(value);
        Collections.sort(origZrange,new ByteArrayComparator());
        int result = origZrange.indexOf(value);
/*        Jedis jedis=null;
        Long temp = null;
        try
        {
            if(toTryCount > 0)
                jedis =  (Jedis) readPool.getResource();
            else
                jedis = writePool.getResource();

            if(toTryCount<redundancyFactor)  assert(jedis.ping().equals("PONG"));      // this means we are in some redundancy scenario

            jedis.watch(key);
            BinaryTransaction bt = jedis.multi();
            bt.select(dbId);
            bt.zadd(key, 0, value);
            Response<Long> rank = bt.zrank(key, value);
            bt.zrem(key, value);
            bt.exec();
            temp = rank.get();

        }

        catch (Exception ex)
        {
            if(toTryCount >0 )
                result  = getInsertionPoint(dbId, key, value, toTryCount - 1);
            else
                throw new HGException("getRankOrInsertionPoint() failed: " + ex.getStackTrace());
        }

        finally
        {
            if(toTryCount>0)
                readPool.returnResource(jedis);
            else
                writePool.returnResource(jedis);
        }
        if(temp == null && toTryCount >0) {
            result = getInsertionPoint(dbId, key, value, toTryCount - 1);
        }

        if (temp != null)
            return temp.intValue();
        else
            return Integer.MIN_VALUE;
            */
        return result;
    }


    public byte[] keySet(int jedisDBID){ return ("keySet"+jedisDBID).getBytes();  }

    private boolean isReachable(String host)
     {
         boolean result = false;
         try                     { result= java.net.InetAddress.getByName(host).isReachable(2000); }
         catch (IOException e)   { System.out.println("java.net.InetAddress.getByName(host).isReachable(2000) threw exception: " + e.getStackTrace().toString());}

         return result;

     }
}