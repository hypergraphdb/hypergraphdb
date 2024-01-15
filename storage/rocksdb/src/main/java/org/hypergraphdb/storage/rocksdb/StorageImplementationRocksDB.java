/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.hypergraphdb.*;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.hypergraphdb.storage.rocksdb.dataformat.FKFVMVDB;
import org.hypergraphdb.storage.rocksdb.dataformat.LogicalDB;
import org.hypergraphdb.storage.rocksdb.dataformat.SVDB;
import org.hypergraphdb.storage.rocksdb.index.IndexManager;
import org.hypergraphdb.storage.rocksdb.resultset.IteratorResultSet;
import org.hypergraphdb.transaction.*;
import org.hypergraphdb.util.HGUtils;
import org.rocksdb.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StorageImplementationRocksDB implements HGStoreImplementation
{

    /*
     TODO when does this need to be loaded?
    */
    static {
        RocksDB.loadLibrary();
    }

    public static final String CF_INCIDENCE = "INCIDENCE";
    public static final String CF_DATA= "DATA";
    public static final String CF_PRIMITIVE = "PRIMITIVE";
    public static final String CF_DEFAULT = "default";


    /*
    A flag which denotes the storage as started and fully initialized.
    Each method accessing the storage must check the flag in
    order to ensure the store is started.

    The flag is set only once -- after all the dependencies are fully initialized.
    Thus, as long as each method accessing the state of the storage checks the flag first,
    they are guaranteed to see the initialized state of the storage.
     */
    private volatile boolean started = false;
    private volatile HashMap<String, LogicalDB> primaryDBs;
    private volatile IndexManager indexManager;
    private int handleSize;
    private OptimisticTransactionDB db;
    private HGStore store;
    private HGConfiguration hgConfig;


   /*
    Mostly use the defaults for DBOptions and column family options.
    This should be good enough to begin with. Tune only if necessary
    and measure performance gains.
    */
    private final DBOptions dbOptions = new DBOptions()
           .setCreateMissingColumnFamilies(true)
           .setCreateIfMissing(true);


    /**
     * @throws HGException if the storage is not started
     */
    private void checkStarted()
    {
        if (!started)
            throw new HGException("The storage layer is not started");
    }


    /*
    TODO
     */
    @Override
    public Object getConfiguration()
    {
        return null;
    }


    /**
     * Get all column family descriptors which are either present or should be present in the database
     * This is called before the start of the DB because we need to explicitly supply all the column families
     * present in the DB.
     * This is so that when the DB is opened, RocksDB knows about the descriptor, which is essentially the options
     * which are not serialized but are needed for a column family from the very beginning -- say comparators
     */
    private List<ColumnFamilyDescriptor> columnFamilyDescriptors() throws RocksDBException
    {
        List<byte[]> columnFamilyIDs;
        try(Options options = new Options())
        {
             columnFamilyIDs = OptimisticTransactionDB.listColumnFamilies(
                    options,
                    Path.of(store.getDatabaseLocation()).toString());
        }

        Set<String> parsedColumnFamilyIDs = columnFamilyIDs
                .stream()
                .map(id -> new String(id, StandardCharsets.UTF_8))
                .collect(Collectors.toSet());


        List<ColumnFamilyDescriptor> descriptors = new LinkedList<>();

        for (String primaryDB : List.of(CF_DEFAULT, CF_PRIMITIVE, CF_DATA, CF_INCIDENCE))
        {
            parsedColumnFamilyIDs.remove(primaryDB);
            descriptors.add(new ColumnFamilyDescriptor(primaryDB.getBytes(StandardCharsets.UTF_8)));
        }

        /*
        Handle the rest of the cf ids which should be indices
         */
        for (String indexCFs : Set.copyOf(parsedColumnFamilyIDs))
        {
            if(IndexManager.isIndexCF(indexCFs))
            {
                parsedColumnFamilyIDs.remove(indexCFs);
                descriptors.add(IndexManager.indexCFD(indexCFs));
            }
        }

        //there should be no other column families existing in the database
        if (!parsedColumnFamilyIDs.isEmpty())
        {
            throw new HGException(String.format(
                    "The following non index column families are present in the db %s",
                    String.join(", ", parsedColumnFamilyIDs)));
        }

        return descriptors;

    }


    @Override
    public synchronized void startup(HGStore store, HGConfiguration configuration)
    {
        if (started)
        {
            return;
        }

        this.store = store;
        this.hgConfig = configuration;
        this.handleSize = configuration.getHandleFactory().nullHandle().toByteArray().length;

        try
        {
            /*
            Get the column family descriptors of the column families
            which are present in the database or are expected to be present.
             */
            List<ColumnFamilyDescriptor> cfDescriptors = columnFamilyDescriptors();

            /*
            open() needs the ColumnFamilyDescriptors of the CFs present in
            the database. It will then populate the cfHandles parameter
            with the handles corresponding to these column families.
             */
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

            this.db = OptimisticTransactionDB.open(
                    dbOptions,
                    Path.of(store.getDatabaseLocation()).toString(),
                    cfDescriptors,
                    cfHandles);
            this.indexManager = new IndexManager(db, this);
            this.primaryDBs = new HashMap<>();

            for (int i = 0; i < cfDescriptors.size(); i++)
            {
                var cfName = new String(cfDescriptors.get(i).getName(), StandardCharsets.UTF_8);
                switch (cfName)
                {
                case CF_DEFAULT: {
                    break;
                }
                case CF_PRIMITIVE:{
                    primaryDBs.put(CF_PRIMITIVE, new SVDB(CF_PRIMITIVE,
                            cfHandles.get(i), cfDescriptors.get(i).getOptions()));
                    break;
                }
                case CF_DATA: {
                    primaryDBs.put(CF_DATA, new SVDB(CF_DATA,
                            cfHandles.get(i), cfDescriptors.get(i).getOptions()));
                    break;
                }
                case CF_INCIDENCE:{
                    primaryDBs.put(CF_INCIDENCE, new FKFVMVDB(CF_INCIDENCE,
                            cfHandles.get(i), cfDescriptors.get(i).getOptions()));
                    break;
                }
                default:{
                    if (IndexManager.isIndexCF(cfName))
                    {
                        this.indexManager.registerColumnFamily(cfName,
                                cfHandles.get(i), cfDescriptors.get(i).getOptions());
                    }
                    else
                    {
                        throw new HGException(String.format("Column family %s is neither primary, nor index", cfName));
                    }
                    break;
                }
                }
            }
            this.started = true;
        }
        catch (RocksDBException e)
        {
            /*
            TODO
                what do we do when opening of the database failed.
                this is probably a critical error
             */
            throw new HGException("Error setting up the database", e);
        }
    }

    @Override
    public synchronized void shutdown()
    {
        if (!this.started)
        {
            return;
        }
        this.started = false;
        this.db.close();
        this.indexManager.close();
        for (LogicalDB primary : this.primaryDBs.values())
        {
            primary.close();
        }
        this.dbOptions.close();
    }

    @Override
    public HGTransactionFactory getTransactionFactory()
    {
        return new HGTransactionFactory()
        {
            @Override
            public HGStorageTransaction createTransaction(
                    HGTransactionContext context,
                    HGTransactionConfig config,
                    HGTransaction parent)
            {
                if (config.isNoStorage())
                {
                    return new VanillaTransaction();
                }
                else
                /*
                TODO support readonly transactions
                 */
                {
                    final WriteOptions writeOptions = new WriteOptions();
                    Transaction parentTxn = null;
                    if (parent != null)
                    {
                        parentTxn = ((RocksDBStorageTransaction)parent.getStorageTransaction()).rocksdbTxn();
                    }
                    /*
                    TODO do we have the correct semantics for the parent transaction?
                     */
                    Transaction txn;

                    /*
                    Set a snapshot to the tx when the transaction begins.
                    This snapshot will be the initial state the transaction  sees and when the transaction is
                    committed, the modified records will be compared against this snapshot and the commit will
                    fail if they are modified outside the transaction after this snapshot. (By default, commit will
                    check that each record is not modified outside the transaction after it is first modified in
                    the transactions)
                    Furthermore, we will support Repeatable Reads isolation by only reading from the transaction's
                    snapshot.
                     */
                    var transactionOptions = new OptimisticTransactionOptions().setSetSnapshot(true);
                    if (parentTxn == null)
                    {
                        txn = db.beginTransaction(writeOptions, transactionOptions);
                    }
                    else
                    {
                        txn = db.beginTransaction(writeOptions, transactionOptions, parentTxn);
                    }
                    return new RocksDBStorageTransaction(txn, writeOptions);
                }
            }

            @Override
            public boolean canRetryAfter(Throwable t)
            {
                return t instanceof TransactionConflictException;
            }
        };
    }

    /**
     *
     * Get the current transaction
     * @return the transaction which is active in the current context
     */
    private RocksDBStorageTransaction txn()
    {
        checkStarted();
        HGTransaction tx = store.getTransactionManager().getContext().getCurrent();
        if (tx == null)
        {
            return RocksDBStorageTransaction.nullTransaction();
        }
        else if (tx.getStorageTransaction() instanceof VanillaTransaction)
        {
            return RocksDBStorageTransaction.nullTransaction();
        }
        else if (tx.getStorageTransaction() instanceof RocksDBStorageTransaction)
        {
            return (RocksDBStorageTransaction) tx.getStorageTransaction();
        }
        else
        {
            throw new HGException("THe current transaction s not created by the " +
                    "supported transaction factory. This is a bug.");
        }
    }


    @Override
    public HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle[] link)
    {
        checkStarted();
        var key = handle.toByteArray();
        var value = fromHandleArray(link);
        this.ensureTransaction(tx -> {
           this.primaryDBs.get(CF_DATA).put(tx, key, value);
           return null;
        });

        return handle;

    }

    @Override
    public HGPersistentHandle store(
            HGPersistentHandle handle,
            byte[] data)
    {
        checkStarted();
        this.ensureTransaction(tx ->
        {
            this.primaryDBs.get(CF_PRIMITIVE).put(tx, handle.toByteArray(), data);
        });
        return handle;
    }

    @Override
    public byte[] getData(HGPersistentHandle handle)
    {
        checkStarted();
        return ensureTransaction(tx -> {
            return this.primaryDBs.get(CF_PRIMITIVE).get(tx, handle.toByteArray());
        });
    }

    @Override
    public HGPersistentHandle[] getLink(HGPersistentHandle handle)
    {
        checkStarted();
        return ensureTransaction(tx -> {
            byte[] bytes = this.primaryDBs.get(CF_DATA).get(tx, handle.toByteArray());
            return bytes == null ? null : toHandleArray(bytes);
        });
    }


    @Override
    public void removeData(HGPersistentHandle handle)
    {
        checkStarted();
        ensureTransaction(tx -> {
            this.primaryDBs.get(CF_PRIMITIVE).delete(tx, handle.toByteArray());
        });
    }


    @Override
    public void removeLink(HGPersistentHandle handle)
    {
        checkStarted();
        ensureTransaction(tx -> {
            this.primaryDBs.get(CF_DATA).delete(tx, handle.toByteArray());
        });
    }


    @Override
    public boolean containsData(HGPersistentHandle handle)
    {
        checkStarted();
        return ensureTransaction(tx -> {
            return this.primaryDBs.get(CF_PRIMITIVE).get(tx, handle.toByteArray()) != null;
        });
    }

    @Override
    public boolean containsLink(HGPersistentHandle handle)
    {
        checkStarted();
        return ensureTransaction(tx -> {
            return this.primaryDBs.get(CF_DATA).get(tx, handle.toByteArray()) != null;
        });
    }

    /*
    TODO
        LMDB uses this logic as well, reuse it
     */
    public byte[] fromHandleArray(HGPersistentHandle[] handles)
    {
        byte [] result = new byte[handles.length * handleSize];
        for (int i = 0; i < handles.length; i++)
            System.arraycopy(handles[i].toByteArray(), 0, result, i*handleSize, handleSize);
        return result;
    }

    /*
    TODO
        LMDB uses this logic as well, reuse it
     */
    private HGPersistentHandle[] toHandleArray(byte[] buffer)
    {
        HGPersistentHandle [] handles = new HGPersistentHandle[buffer.length / handleSize];
        for (int i = 0; i < handles.length; i++)
            handles[i] = this.hgConfig.getHandleFactory().makeHandle(buffer, i*handleSize);
        return handles;
    }

    private HGPersistentHandle toHandle(byte[] buffer)
    {
        if (buffer.length != handleSize)
        {
            throw new HGException(String.format(
                    "Cannot convert a buffer of size %s to a handle. The expected size is %s",
                    buffer.length, handleSize));
        }

        return this.hgConfig.getHandleFactory().makeHandle(buffer);
    }


    @Override
    public HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(
            HGPersistentHandle handle)
    {
        checkStarted();
        return ensureTransaction(tx -> {

            var ri = this.primaryDBs.get(CF_INCIDENCE).iterateValuesForKey(tx, handle.toByteArray());
            return new IteratorResultSet<>(
                    ri.map(bytes -> hgConfig.getHandleFactory().makeHandle(bytes), HGPersistentHandle::toByteArray));
        });


    }



    @Override
    public void removeIncidenceSet(HGPersistentHandle handle)
    {
        /*
        TODO consider range delete instead of iterating over the entire
            index when it is implemented for transctions in java rocks db
         */
        checkStarted();
        ensureTransaction(tx -> {
            this.primaryDBs.get(CF_INCIDENCE).delete(tx, handle.toByteArray());

        });
    }

    @Override
    public long getIncidenceSetCardinality(HGPersistentHandle handle)
    {
        checkStarted();
        try (var rs = ((IteratorResultSet<HGPersistentHandle>) this.getIncidenceResultSet(
                handle)))
        {
            return rs.count();
        }
    }

    @Override
    public void addIncidenceLink(HGPersistentHandle atomHandle,
                HGPersistentHandle incidentLink)
    {
        checkStarted();
        ensureTransaction(tx -> {
            var localKey = atomHandle.toByteArray();
            var value = incidentLink.toByteArray();

            this.primaryDBs.get(CF_INCIDENCE).put(tx, localKey, value);
        });
    }

    @Override
    public void removeIncidenceLink(HGPersistentHandle atomHandle,
            HGPersistentHandle incidentLink)
    {
        checkStarted();
        ensureTransaction(tx -> {
            var localKey = atomHandle.toByteArray();
            var value = incidentLink.toByteArray();

            this.primaryDBs.get(CF_INCIDENCE).delete(tx, localKey, value);
        });

    }


    @Override
    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(
            String name)
    {
        checkStarted();
        return indexManager.getIndex(name);
    }

    @Override
    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(
            String name,
            ByteArrayConverter<KeyType> keyConverter,
            ByteArrayConverter<ValueType> valueConverter,
            Comparator<byte[]> keyComparator,
            Comparator<byte[]> valueComparator,
            boolean isBidirectional,
            boolean createIfNecessary)
    {

        checkStarted();
        return indexManager.getIndex(
                name,
                keyConverter,
                valueConverter,
                keyComparator,
                valueComparator,
                isBidirectional,
                createIfNecessary);

    }

    @Override
    public void removeIndex(String name)
    {
        checkStarted();
        this.indexManager.removeIndex(name);
    }


    /**
     * Execute a function in a transaction. If there is a current transaction,
     * the function will be executed in it.
     * If there is no current transaction, a new one just for the operation will be
     * created unless the enforce transactions config option is set.
     *
     * @param f the function to execute.
     */
    public <T> T ensureTransaction(Function<Transaction, T> f)
    {
        var currentTxn = txn();
        if (currentTxn != null && currentTxn.rocksdbTxn() != null)
            return f.apply(currentTxn.rocksdbTxn());
        else if (this.store.getConfiguration().isEnforceTransactionsInStorageLayer())
            throw new HGException("No current transaction in effect - please use " +
                    "HGTransactionManager.ensureTransaction or turn off transaction enforceability.");
        else
        {
            try (Transaction tx = db.beginTransaction(new WriteOptions()))
            {
                var res = f.apply(tx);
                try
                {
                    tx.commit();
                }
                catch (RocksDBException e)
                {
                    throw new RuntimeException(e);
                }
                return res;
            }
        }
    }



    public void ensureTransaction(Consumer<Transaction> f)
    {
        this.ensureTransaction(tx -> {
            f.accept(tx);
            return null;
        });
    }



    public static void main(String [] argv)
    {
        File location = new File("./storage/dbtest");
//        HGUtils.dropHyperGraphInstance(location.getAbsolutePath());
//        location.mkdirs();
        HGConfiguration config = new HGConfiguration();
        StorageImplementationRocksDB storageImpl =
                new StorageImplementationRocksDB();
        config.setStoreImplementation(storageImpl);
        HGStore store = new HGStore(location.getAbsolutePath(), config);
        HGPersistentHandle handle;
        try
        {
            System.out.println("storing into primitive");
            handle = store.getTransactionManager().ensureTransaction(() -> {
                HGPersistentHandle h = config.getHandleFactory().makeHandle();
                storageImpl.store(h, ("Hello world").getBytes());

                byte[] back = storageImpl.getData(h);
                System.out.println("Read from database: " + new String(back));
                return h;
            });

            System.out.println("printing primitive db");
            storageImpl._printPrimitiveDB();

//            System.out.println("restarting");
//            storageImpl.shutdown();
//            storageImpl.startup(store, config);
//            System.out.println("restarted");

            System.out.println("retrieving stored primitive atoms");
            store.getTransactionManager().ensureTransaction(() -> {
                byte [] back = storageImpl.getData(handle);
                System.out.println(new String(back));
                return handle;
            });

            System.out.println("Storing link data");
            HGPersistentHandle [] linkData = new HGPersistentHandle[] {
                    config.getHandleFactory().makeHandle(),
                    config.getHandleFactory().makeHandle(),
                    config.getHandleFactory().makeHandle()
            };

            HGPersistentHandle otherLink = config.getHandleFactory().makeHandle();
            HGPersistentHandle linkH = store.getTransactionManager().ensureTransaction(() -> {
                storageImpl.store(otherLink, new HGPersistentHandle[]{
                        config.getHandleFactory().makeHandle(),
                        config.getHandleFactory().makeHandle()
                });
                return storageImpl.store(config.getHandleFactory().makeHandle(), linkData);
            });

            System.out.println("Links arrays are equal= " + HGUtils.eq(linkData,
                    store.getTransactionManager().ensureTransaction(() -> {
                        return storageImpl.getLink(linkH);
                    })));

            System.out.println("Links arrays are equal= " + HGUtils.eq(linkData,
                    store.getTransactionManager().ensureTransaction(() -> {
                        return storageImpl.getLink(otherLink);
                    })));

//            if (1==1)
//            {
//                System.out.println("CUT SHORT");
//                return;
//            }

            System.out.println("Storing incidence set ");
            HashSet<HGPersistentHandle> incidenceSet = new HashSet<HGPersistentHandle>();
            for (int i = 0; i < 5; i++)
            {
                incidenceSet.add(config.getHandleFactory().makeHandle());
            }

            store.getTransactionManager().ensureTransaction(() -> {

                var it = incidenceSet.iterator();
                int i = 0;
                while (it.hasNext())
                {
                    var incident = it.next();
                    i++;
                    storageImpl.addIncidenceLink(linkH,
                            (HGPersistentHandle) incident);
                }
                return null;
            });



            _checkIncidence(linkH, incidenceSet, store);

//            HGPersistentHandle removed = incidenceSet.stream().skip(4).findFirst().get();
//            HGPersistentHandle anotherRemoved = incidenceSet.stream().skip(2).findFirst().get();
//            incidenceSet.remove(removed);
//            incidenceSet.remove(anotherRemoved);
//            store.getTransactionManager().ensureTransaction(() -> {
//                storageImpl.removeIncidenceLink(linkH, removed);
//                storageImpl.removeIncidenceLink(linkH, anotherRemoved);
//                return null;
//            });
//
//            checkIncidence(linkH, incidenceSet, store);
//
//            store.getTransactionManager().ensureTransaction(() -> {
//                storageImpl.removeIncidenceSet(linkH);
//                return null;
//            });
//
//            if (store.getTransactionManager().ensureTransaction(() -> storageImpl.getIncidenceSetCardinality(linkH))
//                    != 0)
//                throw new RuntimeException("Incience set for " + linkH + " should be clear.");

        }
        catch (Throwable tx)
        {
            tx.printStackTrace();
        }
        finally
        {
            storageImpl.shutdown();
        }
    }

    /*
    for debugging only
     */
    private static void _checkIncidence(HGPersistentHandle atom, HashSet<HGPersistentHandle> incidenceSet, HGStore store)
    {
        store.getTransactionManager().ensureTransaction(() -> {
            long storedIncidentCount = store.getIncidenceSetCardinality(atom);

            if (storedIncidentCount != incidenceSet.size())
                throw new RuntimeException("Not same number of incident links,  " + storedIncidentCount +
                        ", expecting " + incidenceSet.size());

            try (IteratorResultSet<HGPersistentHandle> rs =
                         (IteratorResultSet<HGPersistentHandle>) store.getIncidenceResultSet(atom))
            {
                while (rs.hasNext())
                {
                    if (!incidenceSet.contains(rs.next()))
                        throw new RuntimeException("Did store incident link: " + rs.current());
                    else
                        System.out.println("Incident " + rs.current() + " is correct.");
                }
            }
            //test the result set

            try (IteratorResultSet<HGPersistentHandle> rs =
                         (IteratorResultSet<HGPersistentHandle>) store.getIncidenceResultSet(atom))
            {
                while (rs.hasNext())
                {
                    if (!incidenceSet.contains(rs.next()))
                        throw new RuntimeException("Did store incident link: " + rs.current());
                    else
                        System.out.println("Incident " + rs.current() + " is correct.");
                }
            }

            try (IteratorResultSet<HGPersistentHandle> rs =
                         (IteratorResultSet<HGPersistentHandle>) store.getIncidenceResultSet(atom))
            {
                System.out.println("Going forward");
                System.out.println("has next is expected to not change the state of the result set");
                System.out.println(rs.hasNext());
                System.out.println(rs.hasNext());
                System.out.println(rs.hasNext());
                System.out.println(rs.hasNext());
                System.out.println(rs.hasNext());
                while (rs.hasNext())
                {
                    if (!incidenceSet.contains(rs.next()))
                        throw new RuntimeException("Did store incident link: " + rs.current());
                    else
                        System.out.println("Incident " + rs.current() + " is correct.");
                }

                rs.goAfterLast();
                System.out.println("Going backwards");
                System.out.println("has prev is expected to not change the state of the result set");
                System.out.println(rs.hasPrev());
                System.out.println(rs.hasPrev());
                System.out.println(rs.hasPrev());
                System.out.println(rs.hasPrev());
                System.out.println(rs.hasPrev());
                while (rs.hasPrev())
                {
                    if (!incidenceSet.contains(rs.prev()))
                        throw new RuntimeException("Did store incident link: " + rs.current());
                    else
                        System.out.println("Incident " + rs.current() + " is correct.");
                }


            }
            return null;
        });
    }



    /**
     * Just print the contents of the
     * primitive db for debugging purposes.
     *
     */
    private void _printPrimitiveDB()
    {
        primaryDBs.get(CF_PRIMITIVE).printDB();
    }


}
