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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ParallelAdd
{
    static File location = new File("/tmp/bdbtest");

    static HGConfiguration config;
    static HGStore store;
    static HGIndex<byte[], byte[]> index;
    static HGStoreImplementation storeImplementation;
    static final String storeImplementationClass = "org.hypergraphdb.storage.bje.BJEStorageImplementation";

    @BeforeClass
    static public void setupAll() throws Exception
    {
        config = new HGConfiguration();
        config.setEnforceTransactionsInStorageLayer(true);
        storeImplementation = (HGStoreImplementation)Class.forName(storeImplementationClass).newInstance();
        config.setStoreImplementation(storeImplementation);
        store = new HGStore(location.getAbsolutePath(), config);
        resetDatabase();
    }

    static void resetDatabase()
    {
        if (index != null)
        {
            try { index.close(); } catch (Throwable t) { }
            store.removeIndex(index.getName());
        }
        index = store.getIndex("paralleladd",
                                    BAtoBA.getInstance(),
                                    BAtoBA.getInstance(),
                        null,
                        null,
                        true);
    }

    @AfterClass
    static public void cleanAll()
    {
        try { store.close(); } catch (Throwable t) {}
        HGUtils.dropHyperGraphInstance(location.getAbsolutePath());
    }

    static <T> T transact(Callable<T> call)
    {
        return store.getTransactionManager().ensureTransaction(call);
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
        int iterations = 10000;
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
                    Throwable t  = transact(/*tx*/ () -> {
                        allvalues.get(key).forEach(value -> {
                            index.addEntry(key, value);
                        });
                        return null;
                    });
                    if (t != null)
                        return t;
                    t = transact(/*tx*/ () -> {
                        HashSet<byte[]> stored = new HashSet<>();
                        HGSearchResult<byte[]> rs = index.find(key);
                        try
                        {
                            if (rs.hasNext())
                                rs.next();
                            if (rs.hasNext())
                                rs.next();
                            try
                            {
                                if (rs.hasNext())
                                    rs.next();
                            }
                            catch (Exception envex)
                            {
                                boolean x = rs.hasNext();
                                throw envex;
                            }
//                            while (rs.hasNext())
//                                stored.add(rs.next());
                        }
                        catch (AssertionError error)
                        {
                            System.err.println("OOPSIES");
                        }
                        finally
                        {
                            HGUtils.closeNoException(rs);
                        }
//                        compareSets(allvalues.get(key).stream().collect(Collectors.toSet()), stored);
                        return null;
                    });
                    return t;
                } catch (Throwable t) { return t; }
                }));
            });
            for (Future<Throwable> future : futures) {
                Throwable t = future.get();
//                if (t != null)
//                    t.printStackTrace(System.out);
            }
            resetDatabase();
        }
    }
}
