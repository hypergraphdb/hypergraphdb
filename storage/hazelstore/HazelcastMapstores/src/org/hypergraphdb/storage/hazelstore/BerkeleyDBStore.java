package org.hypergraphdb.storage.hazelstore;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

// Code source: modified after Wang JunWei, https://github.com/hazelcast/hazelcast/issues/306   (second version)
// code changes markes with #+#

@SuppressWarnings("unchecked")
public class BerkeleyDBStore<K,V> implements MapLoaderLifecycleSupport, MapStore<K,V>, Runnable {
    private final ILogger _logger = Logger.getLogger(BerkeleyDBStore.class.getName());
    private final KryoSerializer kryoserializer = new KryoSerializer();                     // #+# ADDED


    private Database _db; //数据库
    private static Environment _env;
    private static Map _dbMap = new HashMap(); //数据库Map,key是_mapName,value是Database.
    static {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setLocking(true); //true时让Cleaner Thread自动启动,来清理废弃的数据库文件.
        envConfig.setSharedCache(true);
        envConfig.setTransactional(false);
        envConfig.setCachePercent(10); //很重要,不合适的值会降低速度
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "104857600"); //单个log日志文件尺寸是100M

        File file = new File(System.getProperty("user.dir", ".") + "/db/");
        if (!file.exists() && !file.mkdirs()) {
            throw new RuntimeException("Can not create:" + System.getProperty("user.dir", ".") + "/db/");
        }
        _env = new Environment(file, envConfig);
    }

    private int _syncinterval; //同步磁盘间隔,秒.
    private ScheduledExecutorService _scheduleSync; //同步磁盘的Scheduled

    private HazelcastInstance _hazelcastInstance;
    private Properties _properties;
    private String _mapName;

    private Object entryToObject(DatabaseEntry entry) throws Exception {
        int len = entry.getSize();
        if (len == 0) {
            return null;
        } else {
            // return KryoSerializer.read(entry.getData());     // #+# outcommented: What's KryoSerializer?
             return kryoserializer.deserialize(entry.getData(), DatabaseEntry.class);     // #+# ADDED
        }
    }

    private DatabaseEntry objectToEntry(Object object) throws Exception {
        //byte[] bb = KryoSerializer.write(object);     // #+# outcommented: What's KryoSerializer?
        byte[] bb = kryoserializer.serialize(object);     // #+# ADDED

        DatabaseEntry entry = new DatabaseEntry();
        entry.setData(bb);
        return entry;
    }

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        _hazelcastInstance = hazelcastInstance;
        _properties = properties;
        _mapName = mapName;

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(true); //延迟写
        dbConfig.setSortedDuplicates(false);
        dbConfig.setTransactional(false);
        _db = _env.openDatabase(null, _mapName, dbConfig);
        _dbMap.put(_mapName, _db);

        if (_scheduleSync == null) {
            try {
                _syncinterval = Integer.parseInt(_properties.getProperty("syncinterval"));
            } catch (Exception e) {
                _syncinterval = 3;
                _logger.log(Level.WARNING, e.getMessage(), e);
            }
            if (_syncinterval > 0) {
                _scheduleSync = Executors.newSingleThreadScheduledExecutor(); //同步磁盘的Scheduled
                _scheduleSync.scheduleWithFixedDelay(this, 1, _syncinterval, TimeUnit.SECONDS);
            }
        }
        _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":count:" + _db.count());
        _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":初始化完成!");

        //预先把数据加载进Hazelcast集群中
        IMap map = _hazelcastInstance.getMap(mapName);
        Set<K> keySet = privateLoadAllKeys();
        for (K key : keySet) {
            map.putTransient(key, load(key), 0, TimeUnit.SECONDS);
        }
        _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":预先加载数据完成!");

    }

    @Override
    public void destroy() {
        if (_scheduleSync != null) {
            try {
                _scheduleSync.shutdown();
            } finally {
                _scheduleSync = null;
            }
        }

        if (_db != null) {
            try {
                _db.sync();
            } catch (Throwable ex) {
                _logger.log(Level.WARNING, ex.getMessage(), ex);
            }

            _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":count:" + _db.count());
            try {
                _db.close();
            } catch (Throwable ex) {
                _logger.log(Level.WARNING, ex.getMessage(), ex);
            } finally {
                _db = null;
                _dbMap.remove(_mapName);
            }
            _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":销毁完成!");
        }

        if (_dbMap.size() == 0) {
            try {
                boolean anyCleaned = false;
                while (_env.cleanLog() > 0) {
                    anyCleaned = true;
                }
                if (anyCleaned) {
                    CheckpointConfig force = new CheckpointConfig();
                    force.setForce(true);
                    _env.checkpoint(force);
                }
            } catch (Throwable ex) {
                _logger.log(Level.WARNING, ex.getMessage(), ex);
            }

            try {
                _env.close();
            } catch (Throwable ex) {
                _logger.log(Level.WARNING, ex.getMessage(), ex);
            } finally {
                _env = null;
            }
            _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":BerkeleyDB数据库关闭!");
        }

    }

    @Override
    //定时将内存中的内容写入磁盘
    public void run() {
        try {
            _db.sync();
        } catch (Throwable ex) {
            _logger.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    @Override
    public V load(K key) {
        try {
            DatabaseEntry keyEntry = objectToEntry(key);
            DatabaseEntry valueEntry = new DatabaseEntry();
            OperationStatus status = _db.get(null, keyEntry, valueEntry, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS) {
                return (V) entryToObject(valueEntry);
            } else {
                return null;
            }
        } catch (Exception e) {
            _logger.log(Level.SEVERE, e.getMessage(), e);
            return null;
        }
    }

    @Override
    public void delete(K key) {
        try {
            _db.delete(null, objectToEntry(key));
            if (_syncinterval == 0) {
                _db.sync();
            }
        } catch (Exception e) {
            _logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        for (K key : keys) {
            this.delete(key);
        }
        if (_syncinterval == 0) {
            _db.sync();
        }
    }

    @Override
    public void store(K key, V value) {
        try {
            DatabaseEntry keyEntry = objectToEntry(key);
            DatabaseEntry valueEntry = objectToEntry(value);
            _db.put(null, keyEntry, valueEntry);
            if (_syncinterval == 0) {
                _db.sync();
            }
        } catch (Exception e) {
            _logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    @Override
    public void storeAll(Map<K,V> map) {
        for (Entry<K,V> entrys : map.entrySet()) {
            this.store(entrys.getKey(), entrys.getValue());
        }
        if (_syncinterval == 0) {
            _db.sync();
        }
    }

    @Override
    public Map loadAll(Collection keys) {
        //return privateLoadAll(keys);

        return null;
    }

    private Map privateLoadAll(Collection<K> keys) {
        Map map = new java.util.HashMap(keys.size());
        for (K key : keys) {
            map.put(key, this.load(key));
        }
        return map;
    }

    @Override
    public Set loadAllKeys() {
        //return privateLoadAllKeys();

        return null;
    }

    private Set privateLoadAllKeys() {
        Set keys = new java.util.HashSet((int) _db.count());
        Cursor cursor = null;
        try {
            cursor = _db.openCursor(null, null);
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                keys.add((K) entryToObject(foundKey));
            }
        } catch (Exception e) {
            _logger.log(Level.SEVERE, e.getMessage(), e);
        } finally {
            cursor.close();
        }

        _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":loadAllKeys:" + keys.size());

        return keys;
    }
}
