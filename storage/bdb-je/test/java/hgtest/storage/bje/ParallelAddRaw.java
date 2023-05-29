package hgtest.storage.bje;

import com.sleepycat.je.*;
import org.hypergraphdb.*;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import org.hypergraphdb.storage.bje.SingleKeyResultSet;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.TransactionConflictException;
import org.hypergraphdb.util.HGUtils;
import org.junit.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ParallelAddRaw
{
    static File location = new File("/tmp/bdbtest");
    static Environment env;
    static Database db;

    @BeforeClass
    static public void setupAll()
    {
        HGUtils.dropHyperGraphInstance(location.getAbsolutePath());
        location.mkdirs();
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS, "5");
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setCachePercent(30);
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
                Long.toString(10000000 * 10l));
        envConfig.setConfigParam(
                EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE,
                Long.toString(1024 * 1024));
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_READ_SIZE,
                Long.toString(1024 * 1024));
        envConfig.setTxnSerializableIsolation(true);
        envConfig.setTxnSerializableIsolationVoid(true);
        envConfig.setLockTimeout(100, TimeUnit.MILLISECONDS);

        Durability defaultDurability = new Durability(
                Durability.SyncPolicy.WRITE_NO_SYNC,
                Durability.SyncPolicy.NO_SYNC, // unused by non-HA applications.
                Durability.ReplicaAckPolicy.NONE); // unused by non-HAapplications.
        envConfig.setDurability(defaultDurability);

        env = new Environment(location, envConfig);
        resetDatabase();
    }

    static void resetDatabase()
    {
        if (db != null)
        {
            db.close();
            env.removeDatabase(null, "testparallel");
        }
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setSortedDuplicates(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        db = env.openDatabase(null, "testparallel", dbConfig);
    }

    @AfterClass
    static public void cleanAll()
    {
        try { db.close(); } catch (Throwable t) {}
        try { env.close(); } catch (Throwable t) {}
    }

    static <T> T transact(Function<Transaction, T> call)
    {
        while (true) {
            TransactionConfig txConfig = new TransactionConfig();
            Transaction tx = env.beginTransaction(null, txConfig);
            try {
                T result = call.apply(tx);
                tx.commit();
                return result;
            } catch (Throwable t) {
                try {
                    tx.abort();
                } catch (Throwable tt) {
                }
                if (!(t instanceof TransactionConflictException))
                    if (t instanceof RuntimeException)
                        throw (RuntimeException) t;
                    else
                        throw new RuntimeException(t);
            }
        }
    }

    public static byte [] randomBytes(int count)
    {
        Random random = new Random();
        byte [] bytes = new byte[count];
        random.nextBytes(bytes);
        return bytes;
    }

    public static DatabaseEntry entry(byte [] data)
    {
        return new DatabaseEntry(data);
    }

    public static void compareSets(Set<byte[]> expected, Set<byte[]> actual)
    {
        Assert.assertEquals(expected.size(), actual.size());
        for (byte[] bytes : expected)
        {
            Assert.assertTrue(actual.stream().filter(
                    other -> Arrays.compare(bytes, other) == 0).findFirst().isPresent());
        }
    }

    @Ignore
    @Test
    public void testConcurrentAddition() throws Exception
    {
        int iterations = 1000;
        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        HashSet<byte[]> keySet = new HashSet<>();
        for (int i = 0; i < threadCount; i++)
            keySet.add(randomBytes(10));

        HashMap<byte[], List<byte[]>> allvalues = new HashMap<byte[], List<byte[]>>();

        for (int i = 0; i < iterations; i++)
        {
            keySet.forEach(key -> {
                allvalues.put(key, Arrays.asList(randomBytes(10), randomBytes(15)));
            });

            ArrayList<Future<Throwable>> futures = new ArrayList<Future<Throwable>>();
            keySet.forEach(key -> {
                futures.add(executorService.submit(() -> { try {
                    Throwable t  = transact(tx -> {
                        allvalues.get(key).forEach(value -> {
                            OperationStatus status = db.putNoDupData(tx, entry(key), entry(value));
                            if (status != OperationStatus.KEYEXIST && status != OperationStatus.SUCCESS)
                                throw new RuntimeException("Can't add status = " + status);
                        });
                        return null;
                    });
                    if (t != null)
                        return t;
                    t = transact(tx -> {
                        HashSet<byte[]> stored = new HashSet<>();
                        CursorConfig cursorConfig = new CursorConfig();
                        Cursor cursor = db.openCursor(tx, cursorConfig);
                        DatabaseEntry value = new DatabaseEntry();
                        OperationStatus status = cursor.getSearchKey(entry(key), value, LockMode.DEFAULT);
                        while (status == OperationStatus.SUCCESS) {
                            byte[] data = new byte[value.getSize()];
                            System.arraycopy(value.getData(), value.getOffset(), data, 0, value.getSize());
                            stored.add(data);
                            value = new DatabaseEntry();
                            status = cursor.getNextDup(entry(key), value, LockMode.DEFAULT);
                        }
                        try {
                            cursor.close();
                        } catch (Throwable tt) {
                        }
                        compareSets(allvalues.get(key).stream().collect(Collectors.toSet()), stored);
                        return null;
                    });
                    return t;
                } catch (Throwable t) { return t; }
                }));
            });
            for (Future<Throwable> future : futures) {
                Throwable t = future.get();
                if (t != null)
                    t.printStackTrace();
            }
            resetDatabase();
        }
    }
}
