package hgtest.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import hgtest.HGTestBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.TransactionConflictException;
import org.hypergraphdb.transaction.TxMap;
import org.hypergraphdb.util.HGUtils;

public class StorageTxTest extends HGTestBase
{
    private int itemCount = 200;
    private int threadCount = 20;
    private Map<Integer, HGPersistentHandle> handleMap = new HashMap<Integer, HGPersistentHandle>();
    private TxMap<HGPersistentHandle, Integer> data = null;    
    private ArrayList<Throwable> errors = new ArrayList<Throwable>();


    public void verifyData()
    {
        for (int i = 0; i < itemCount; i++)
        {
            byte [] data = graph.getStore().getData(handleMap.get(i));
            assertNotNull(data);
            assertEquals(BAUtils.readInt(data, 0), i + threadCount);
        }
    }

    private void increment(final HGPersistentHandle dataHandle)
    {        
        final HGTransactionManager txman = graph.getTransactionManager();
        int mismatches = 0;
        int aborts = 0;
        int abortmismatch = 0;
        int storageAbort = 0;
        int memAbort = 0;
        while (true)
        {
            txman.beginTransaction();
//            HGTransaction tx = txman.getContext().getCurrent();
            boolean mismatch = false;
            try
            {
                byte [] B = new byte[4]; 
                B = graph.getStore().getData(dataHandle);
                int value = data.get(dataHandle);
                int storageValue = BAUtils.readInt(B, 0);
                // final byte [] B = new byte[4];                
                if (value != storageValue)
                {
//                    System.out.println("oops, cache differs from storage! " + value + ", " + storageValue);
                    mismatch = true;
                    mismatches++;
                }
                int newvalue = value + 1;                
                data.put(dataHandle, newvalue);
                BAUtils.writeInt(newvalue, B, 0);
                graph.getStore().store(dataHandle, B);      
            }
            catch (Throwable t)
            {
                t = HGUtils.getRootCause(t);
      			    if (!graph.getStore().getTransactionFactory().canRetryAfter(t))
                {
                    System.err.println("Exception during transaction!");
                    throw new RuntimeException(t);
                }
                if (t instanceof TransactionConflictException)
                    memAbort++;
                else
                    storageAbort++; 
                aborts++;
                if (mismatch)
                {
//                    if (! (t instanceof TransactionConflictException))
//                        System.err.println("mismatch on bdb conflict");
                    abortmismatch++;
                }
//                    System.out.println("Aborted on mismatch.");
                try { txman.endTransaction(false); }
                catch (Throwable tt) { tt.printStackTrace(System.err); }
                continue;
            }
            try
            {
                txman.endTransaction(true);
                break;
            }
            catch (Throwable t)
            {
                t = HGUtils.getRootCause(t);
      			    if (!graph.getStore().getTransactionFactory().canRetryAfter(t))
                {
                    System.err.println("Exception during commit!");
                    throw new RuntimeException(t);
                }
                if (t instanceof TransactionConflictException)
                    memAbort++;
                else
                {
                    storageAbort++;
                    System.err.println("bdb conflict detected.");
                }
                aborts++;
                if (mismatch)
                {
                    if (! (t instanceof TransactionConflictException))
                        System.err.println("mismatch on bdb conflict");
                    abortmismatch++;
                    System.out.println("mismatch at commit.");
                }
//                    System.out.println("Aborted on mismatch.");
            }
        }
//        if (mismatches > 0)
//            System.out.println("abort/mis = " + abortmismatch + ", aborts = " + aborts + ", mismatches = " + mismatches);
        if (mismatches != abortmismatch)
            System.err.println("mismatch with abort");
//        txman.transact(new Callable<Integer>() 
//        { 
//            public Integer call()
//            {                
//                byte [] B = new byte[4]; 
//                B = graph.getStore().getData(dataHandle);
//                int value = data.get(dataHandle);
//                int storageValue = BAUtils.readInt(B, 0);
//                // final byte [] B = new byte[4];                
//                if (value != storageValue)
//                    System.out.println("oops, cache differs from storage! " + value + ", " + storageValue);
//                int newvalue = value + 1;                
//                data.put(dataHandle, newvalue);
//                BAUtils.writeInt(newvalue, B, 0);
//                graph.getStore().store(dataHandle, B);
//                return null;
//            }
//        });
    }

    private void incrementValues()
    {
        for (int i = 0; i < itemCount; i++)
        {
            HGPersistentHandle handle = handleMap.get(i);
            increment(handle);
        }
    }

    // @Test
    public void testConcurrentLinkCreation()
    {
        data = new TxMap<HGPersistentHandle, Integer>(graph.getTransactionManager(), null);
        byte [] buffer = new byte[4];
        for (int i = 0; i < itemCount; i++)
        {
            HGPersistentHandle handle = graph.getHandleFactory().makeHandle(); 
            handleMap.put(i, handle);
            BAUtils.writeInt(i, buffer, 0);
            data.put(handle, i);
            graph.getStore().store(handle, buffer);
        }
        ExecutorService pool = Executors.newFixedThreadPool(10);
        for (int i = 0; i < threadCount; i++)
        {
            pool.execute(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        incrementValues();
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace(System.err);
                        errors.add(t);
                    }
                }
            });
        }
        try
        {
            pool.shutdown();
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            System.out.println("testTxMap interrupted.");
            return;
        }
        assertEquals(errors.size(), 0);
//        this.reopenDb();
        verifyData();
    }

    public static void main(String[] argv)
    {
        for (int i = 0; i < 100; i++)
        {
            StorageTxTest test = new StorageTxTest();
            HGUtils.dropHyperGraphInstance(test.getGraphLocation());
            test.setUp();        
            try
            {
                test.testConcurrentLinkCreation();
            }
            catch (Throwable t)
            {
                t.printStackTrace(System.err);
                break;
            }
            finally
            {
                test.tearDown();
            }
            System.out.println("Done with iteration " + i);
        }
    }
}