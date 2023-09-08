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
import org.hypergraphdb.transaction.*;
import org.hypergraphdb.util.HGUtils;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class StorageImplementationRocksDB implements HGStoreImplementation
{

    private static final String DB_ROOT = "data";
    private static final String DATA_DB_NAME = "datadb";
    private static final String PRIMITIVE_DB_NAME = "primitivedb";
    private static final String INCIDENCE_DB_NAME = "incidencedb";

    /*
    TODO when does this need to be loaded?
     */
    static {
        RocksDB.loadLibrary();
    }



    private TransactionDB db;

    private DBOptions dbOptions;
    private Options options;
    private TransactionDBOptions txoptions;
    private Filter bloomfilter;
    private ReadOptions readOptions;
    private Statistics stats;
    private RateLimiter rateLimiter;
    private HGStore store;
    private HGConfiguration hgConfig;
    private int handleSize;

    public static enum ColumnFamily
    {
        PRIMITIVE, DATA, INCIDENCE;
        private ColumnFamilyHandle handle;
        private ColumnFamilyDescriptor descriptor;

        private void initialize(ColumnFamilyHandle handle, ColumnFamilyDescriptor descriptor)
        {
            if (handle == null && descriptor == null)
            {
                throw new IllegalArgumentException("Arguments cannot be null");
            }
            this.handle = handle;
            this.descriptor = descriptor;
        }

        public boolean isInitialized()
        {
            return this.handle != null;
        }

        public ColumnFamilyHandle handle()
        {
            return handle;
        }
    }

    /*


     Since RocksDB is a linear data structure without multiple values for
     a key, we need
        1. a way to split that structure in different substructures ('tables')
        2. a way to associate several values to a given key

        From the users POV (thus API) we have (KEY, VALUE) which is part of a given DATABASE.
        Assuming the logical database supports multiple values for a key,
        the record logical record is represented by a physical RocksDB record
        in the format
        key: <DATABASE_PREFIX>_<KEY>_<VALUE>
        value: null


     A note on keys.

    'Key' can mean three different things in different situations:


        1. local key -- this is a key from a user point of view. e.g. this
            is the handle.toByteArray() or indexed_value.toByteArray().
            Conceptually, we have several databases and in each of which, we
            can call database.get(logicalKey) and this will return all the values
            for a given logical key, contained in that database.

        2. global key ::= databaseId + logicalKey
            unique key for the entire store. the different 'databases' can have
            duplicate keys, so this is the local key, prefixed with the database id
            This is the level at which we have the

        3. rocksDBKey
            ::= databaseKey + value (for multivalued tables)
            ::= databaseKey (for single valued tables)

            That's the 'physical' key we will use to store int the rocksDB
            database


     */


    @Override
    public Object getConfiguration()
    {
        return null;
    }


    /**
     * Get all column family descriptors in the database
     * @return
     * @throws RocksDBException
     */
    private List<ColumnFamilyDescriptor>getColumnFamilyDescriptors() throws RocksDBException
    {
        /*
        The ids of the column families
         */
        return TransactionDB.listColumnFamilies(options, Path.of(store.getDatabaseLocation()).toString())
                .stream()
                .map(cfId -> new ColumnFamilyDescriptor(
                        cfId,
                        new ColumnFamilyOptions()))
                .collect(Collectors.toList());
    }

    private void setup()
    {
        /*
        All of the options below are from the sample application;
        TODO investigate what they do and verify if they  are correct
        TODO move to a config object
         */
        this.options = new Options();
        this.dbOptions = new DBOptions();
        this.txoptions = new TransactionDBOptions();
        this.bloomfilter = new BloomFilter(10);
        this.readOptions = new ReadOptions().setFillCache(false);
        this.stats = new Statistics();
        this.rateLimiter = new RateLimiter(10_000_000, 10_000, 10);

        options.setCreateIfMissing(true).setStatistics(stats)
                .setWriteBufferSize(8 * SizeUnit.KB)
                .setMaxWriteBufferNumber(3).setMaxBackgroundJobs(10)
                .setCompressionType(CompressionType.ZLIB_COMPRESSION)
                .setCompactionStyle(CompactionStyle.UNIVERSAL);

//        options.setMemTableConfig(
//                new HashSkipListMemTableConfig().setHeight(3)
//                        .setBranchingFactor(3).setBucketCount(2000000));
//        options.setMemTableConfig(
//                new HashLinkedListMemTableConfig().setBucketCount(99999));
//
//        options.setMemTableConfig(
//                new VectorMemTableConfig().setReservedSize(9999));

        options.setMemTableConfig(new SkipListMemTableConfig());

        options.setTableFormatConfig(new PlainTableConfig());
        options.setAllowMmapReads(true);
        options.setRateLimiter(rateLimiter);

        final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        Cache cache = new LRUCache(64 * 1024, 6);
        tableOptions.setBlockCache(cache).setNoBlockCache(false)
                .setFilterPolicy(bloomfilter).setBlockSizeDeviation(5)
                .setBlockRestartInterval(10)
                .setCacheIndexAndFilterBlocks(true);

        options.setTableFormatConfig(tableOptions);

    }

    @Override
    public void shutdown()
    {
        this.options.close();
        this.dbOptions.close();
        this.txoptions.close();
        this.bloomfilter.close();
        this.readOptions.close();
        this.stats.close();
        this.rateLimiter.close();
        this.db.close();
    }


    /*
    TODO use the supplied configuration to set the needed options;
        determine which options are needed -- Options, DBOptions, ReadOptions,
        TransactionOptions etc.
     */
    @Override
    public void startup(HGStore store, HGConfiguration configuration)
    {
        this.store = store;
        this.hgConfig = configuration;
        this.handleSize = configuration.getHandleFactory().nullHandle().toByteArray().length;

        setup();
        try
        {
            /*
            Getting the column family descriptors happens before we open the
            database itself
             */
            /*
            get the names of the existing column families in the database.
            this happens before opening the database

             */
            List<ColumnFamilyDescriptor> cfDescriptors = getColumnFamilyDescriptors();

            /*
            The call to open will fill this with the column family handles
             */
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            this.db = TransactionDB.open(
                    dbOptions,
                    txoptions,
                    Path.of(store.getDatabaseLocation()).toString(),
                    cfDescriptors,
                    cfHandles);

            ensureColumnFamilies(cfDescriptors, cfHandles);
//            this.incidencedb = RocksDB.open(options, new File(new File(DB_ROOT), INCIDENCE_DB_NAME).getAbsolutePath());
//            this.primitivedb = RocksDB.open(options, new File(new File(DB_ROOT), PRIMITIVE_DB_NAME).getAbsolutePath());
        }
        catch (RocksDBException e)
        {
            /*
            TODO
                what do we do when opening of the database failed.
                this is probably a critical error
             */
            throw new RuntimeException(e);
        }
    }


    /**
     * Ensure all expected column families are present in the database
     *
     *  TODO
     *      we are ensuring the existence of the PRIMITIVE, DATA, and INCIDENT
     *      CFs. we will need CFs for the indices as well
     *
     * @param cfDescriptors the descriptors of the existing CFs
     * @param cfHandles the handles of the existing cfs
     * @throws RocksDBException
     */
    private void ensureColumnFamilies(
            List<ColumnFamilyDescriptor> cfDescriptors,
            List<ColumnFamilyHandle> cfHandles) throws RocksDBException
    {
        if (cfHandles.size() != cfDescriptors.size())
        {
            throw new RuntimeException("The sizes have to be the same");
        }
        for (int i = 0; i < cfDescriptors.size(); i++)
        {
            var desc =  cfDescriptors.get(i);
            var handle = cfHandles.get(i);
            String cfName = new String(desc.getName());

            /*
            we will need custom named column families and this approach does
            not support that
             */
            if ("default".equalsIgnoreCase(cfName))
                continue;

            ColumnFamily cf = ColumnFamily.valueOf(cfName);

            if (!cf.isInitialized())
            {
                cf.initialize(handle, desc);
            }
            else
            {
                throw new RuntimeException(String.format(
                        "column family %s is already initialized.",
                        cfName));
                //problem
            }
        }
        /*
        if some column families are not initialized from the existing
        database, create them
         */
        if (!ColumnFamily.PRIMITIVE.isInitialized())
        {
            ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(
                    ColumnFamily.PRIMITIVE.name().getBytes(StandardCharsets.UTF_8));
            var handle = db.createColumnFamily(descriptor);
            ColumnFamily.PRIMITIVE.initialize(handle, descriptor);
        }

        if (!ColumnFamily.DATA.isInitialized())
        {
            ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(
                    ColumnFamily.DATA.name().getBytes(StandardCharsets.UTF_8));
            var handle = db.createColumnFamily(descriptor);
            ColumnFamily.DATA.initialize(handle, descriptor);
        }

        if (!ColumnFamily.INCIDENCE.isInitialized())
        {
            ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(
                    ColumnFamily.INCIDENCE.name().getBytes(StandardCharsets.UTF_8));
            var handle = db.createColumnFamily(descriptor);
            ColumnFamily.INCIDENCE.initialize(handle, descriptor);
        }



    }



    @Override
    public HGTransactionFactory getTransactionFactory()
    {
        return new HGTransactionFactory()
        {
            @Override
            public HGStorageTransaction createTransaction(
                    HGTransactionContext context, HGTransactionConfig config,
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
                    /*
                    TODO
                        this needs to be closed, but when? Probably when
                        the transaction is committed / rolled back
                        right now we close the options in the transaction
                        itself
                     */
                    /*
                    TODO how do we set the transaction as readonly?
                        Set snapshot here?

                     */
                    final TransactionOptions txnOptions = new TransactionOptions().setSetSnapshot(true);
                    final WriteOptions writeOptions = new WriteOptions();
//                    var parentTxn = ((StorageTransactionRocksDB)parent.getStorageTransaction()).txn();
                    /*
                    TODO do we have the correct semantics for the parent transaction?
                    TODO in RocksDB the transaction is created in the
                     */
                    final Transaction txn = db.beginTransaction(writeOptions, txnOptions);
                    /*
                    TODO is this the place to take a snapshot?
                        What should the repeatable read logic be?
                        When should we take snapshots
                     */
//                    txn.setSnapshot(); I think the snapshot is automatically taken when the transaction is created
                    return new StorageTransactionRocksDB(txn, txnOptions, writeOptions);

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
     * Get the current transaction
     * @return the transaction which is active in the current context
     */
    private StorageTransactionRocksDB txn()
    {
        HGTransaction tx = store.getTransactionManager().getContext().getCurrent();
        if (tx == null)
            return StorageTransactionRocksDB.nullTransaction();
        else if (tx.getStorageTransaction() instanceof StorageTransactionRocksDB)
        {
            return (StorageTransactionRocksDB) tx.getStorageTransaction();
        }
        else
        {
            throw new HGException("THe current transaction s not created by the " +
                    "supported transaction factor. This is a bug.");
        }
    }



    @Override
    public HGPersistentHandle store(
            HGPersistentHandle handle,
            HGPersistentHandle[] link)
    {
        var key = handle.toByteArray();
        var value = fromHandleArray(link);
        try
        {
            txn().rocksdbTxn().put(ColumnFamily.DATA.handle(), key, value);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }

        return handle;

    }

    @Override
    public HGPersistentHandle store(
            HGPersistentHandle handle,
            byte[] data)
    {
        try
        {
            txn().rocksdbTxn().put(ColumnFamily.PRIMITIVE.handle(), handle.toByteArray(), data);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
        return handle;
    }

    @Override
    public byte[] getData(HGPersistentHandle handle)
    {
        try
        {
            var res = txn().rocksdbTxn().get(ColumnFamily.PRIMITIVE.handle(), readOptions, handle.toByteArray());

//            var stats = this.db.getApproximateMemTableStats(new Range(
//                    new Slice(LogicalDatabase.primitive().firstGlobalKeyInDB()),
//                    new Slice(LogicalDatabase.primitive().lastGlobalKeyInDB())));
//
//            long size = this.db.getApproximateSizes(List.of(new Range(
//                    new Slice(LogicalDatabase.primitive().firstGlobalKeyInDB()),
//                    new Slice(LogicalDatabase.primitive().lastGlobalKeyInDB()))),
//                    SizeApproximationFlag.INCLUDE_FILES, SizeApproximationFlag.INCLUDE_MEMTABLES)[0];

//            System.out.println("memtable count: " + stats.count);
//            System.out.println("appr sizes: " + size/33.0);

//            try (var ro = new ReadOptions()
//                    .setIterateLowerBound(new Slice(LogicalDatabase.primitive().firstGlobalKeyInDB()))
//                    .setIterateUpperBound(new Slice(LogicalDatabase.primitive().lastGlobalKeyInDB())))
//            {
//                var iterator = txn().rocksdbTxn().getIterator(ro);
//                iterator.seek(LogicalDatabase.primitive().scopeKey(handle.toByteArray(), null));
////                System.out.println(String.format( "value from iterator %s", new String(iterator.value())));
//
//
//            }

            System.out.println(new String(res));
//            datadb.getA
            return res;
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }

    public HGPersistentHandle[] getLink(HGPersistentHandle handle)
    {
        try
        {
            byte[] bytes = txn().rocksdbTxn().get(ColumnFamily.DATA.handle(), readOptions, handle.toByteArray());
            return toHandleArray(bytes);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }

    /**
     * Remove a rocks db key from the database
     * @param rocksDBKey the rocks db key (i.e. prefixed to a given database and concatenated
     *  with the value, if the database supports multivalued keys) to remove
     */
    private void removeRocksDBKey(byte[] rocksDBKey)
    {
        try
        {
            txn().rocksdbTxn().delete(rocksDBKey);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }

    @Override
    public void removeData(HGPersistentHandle handle)
    {
        try
        {
            txn().rocksdbTxn().delete(ColumnFamily.PRIMITIVE.handle(), handle.toByteArray());
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }


    @Override
    public void removeLink(HGPersistentHandle handle)
    {
        try
        {
            txn().rocksdbTxn().delete(ColumnFamily.DATA.handle(), handle.toByteArray());
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }

    /**
     * Whether the database contains a given key
     * @param rocksDBkey the rocksDB key to check (i.e. prefixed to a given database and concatenated
     *  with the value, if the database supports multivalued keys)
     * @return true if the database contains that key
     */
    private boolean containsRocksDBKey(byte[] rocksDBkey)
    {
        try
        {
            return txn().rocksdbTxn().get(readOptions, rocksDBkey) != null;
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }

    }

    @Override
    public boolean containsData(HGPersistentHandle handle)
    {
        try
        {
            return txn().rocksdbTxn().get(
                    ColumnFamily.PRIMITIVE.handle(),
                    readOptions,
                    handle.toByteArray() ) != null;
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }

    @Override
    public boolean containsLink(HGPersistentHandle handle)
    {
        try
        {
            return txn().rocksdbTxn().get(
                    ColumnFamily.DATA.handle(),
                    readOptions,
                    handle.toByteArray() ) != null;
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
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
            throw new HGException(String.format("Cannot convert a buffer of size %s to a handle. The expected size is %s", buffer.length, handleSize));
        }

        return this.hgConfig.getHandleFactory().makeHandle(buffer);
    }


    @Override
    public HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(
            HGPersistentHandle handle)
    {

        byte[] start =  LogicalDatabase.incidence().firstGlobalKeyInDB();
        byte[] end =  LogicalDatabase.incidence().lastGlobalKeyInDB();
        /*
        TODO how do we attach the iterator to the transaction?
         */
        try (var ro = new ReadOptions().setIterateUpperBound(new Slice(end)))
        {
            var iterator = txn().rocksdbTxn().getIterator(ro);
            //
            byte[] rocksDBKey = LogicalDatabase.incidence()
                    .scopeKey(handle.toByteArray(),
                            new byte[LogicalDatabase.VALUE_KEY_SIZE]);

            iterator.seek(rocksDBKey);

            long size = db.getApproximateMemTableStats(new Range(new Slice(start), new Slice(end))).count;

            return new SingleKeyRocksDBResultSet<>(db,
                    LogicalDatabase.incidence(),
                    iterator,
                    handle.toByteArray(),
                    HGPersistentHandle::toByteArray,
                    bytes -> hgConfig.getHandleFactory().makeHandle(bytes));

        }
    }

    @Override
    public void removeIncidenceSet(HGPersistentHandle handle)
    {

    }

    @Override
    public long getIncidenceSetCardinality(HGPersistentHandle handle)
    {
        return ((IndexResultSet<?>)this.getIncidenceResultSet(handle)).count();
    }

    @Override
    public void addIncidenceLink(HGPersistentHandle atomHandle,
                HGPersistentHandle incidentLink)
    {
        var localKey = atomHandle.toByteArray();
        var value = incidentLink.toByteArray();
        var rocksDBkey = LogicalDatabase.incidence().scopeKey(localKey, value);
        try
        {
            db.put(rocksDBkey, new byte[0]);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }

    @Override
    public void removeIncidenceLink(HGPersistentHandle atomHandle,
            HGPersistentHandle incidentLink)
    {
        var localKey = atomHandle.toByteArray();
        var value = incidentLink.toByteArray();
        var rocksDBkey = LogicalDatabase.incidence().scopeKey(localKey, value);
        try
        {
            db.delete(rocksDBkey);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }

    }

    static void checkIncidence(HGPersistentHandle atom, HashSet<HGPersistentHandle> incidenceSet, HGStore store)
    {
        store.getTransactionManager().ensureTransaction(() -> {
            long storedIncidentCount = store.getIncidenceSetCardinality(atom);

//            if (storedIncidentCount != incidenceSet.size())
//                throw new RuntimeException("Not same number of incident links,  " + storedIncidentCount +
//                        ", expecting " + incidenceSet.size());

            try (HGRandomAccessResult<HGPersistentHandle> rs = store.getIncidenceResultSet(atom))
            {
                while (rs.hasNext())
                    if (!incidenceSet.contains(rs.next()))
                        throw new RuntimeException("Did store incident link: " + rs.current());
                    else
                        System.out.println("INcident " + rs.current() + " is correct.");
            }
            return null;
        });
    }

    @Override
    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(
            String name)
    {
        return null;
    }

    @Override
    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(
            String name, ByteArrayConverter<KeyType> keyConverter,
            ByteArrayConverter<ValueType> valueConverter,
            Comparator<byte[]> keyComparator,
            Comparator<byte[]> valueComparator, boolean isBidirectional,
            boolean createIfNecessary)
    {
        return null;
    }

    @Override
    public void removeIndex(String name)
    {

    }

    void printdb()
    {
        var i = this.db.newIterator();
        i.seekToFirst();

        while (i.isValid())
        {
            var k = i.key();
            var v = i.value();
            System.out.println();
            switch (k[0]) {
            case (byte)0 : {
                var parserdKey = this.toHandle(
                        Arrays.copyOfRange(k, 1, 17));
                var parsedValue = new String(v);
                System.out.printf("primitive; key(%s); value(%s) %n", parserdKey, parsedValue);

                //primitive
                break;
            }
            case (byte)1 : {
                var parserdKey = this.toHandle(
                        Arrays.copyOfRange(k, 1, 17));
                var parsedValue = Arrays.stream(this.toHandleArray(v)).map(
                        Object::toString).collect(Collectors.joining(","));
                System.out.printf("data; key(%s); value(%s) %n", parserdKey, parsedValue);

                //primitive
                break;
            }
            case (byte)2 : {
                var parserdKey = this.toHandle(
                        Arrays.copyOfRange(k, 1, 17));
                var parserdValue = this.toHandle(
                        Arrays.copyOfRange(k, 17, 33));
                System.out.printf("incidence; key(%s); value(%s) %n", parserdKey, parserdValue);
                break;
            }
            }
            i.next();
        }
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
        try
        {
            for (int i = 0; i < 1_000_000; i++)
            {
                int j = i;
                store.getTransactionManager().ensureTransaction(() -> {

                    HGPersistentHandle h = config.getHandleFactory().makeHandle();
                    storageImpl.store(h, ("Hello world" + j).getBytes());

                    byte[] back = storageImpl.getData(h);
//                    System.out.println(new String(back));

                    return null;
                });
            }


            storageImpl.shutdown();
            storageImpl.startup(store, config);

//            store.getTransactionManager().ensureTransaction(() -> {
//                byte [] back = storageImpl.getData(h);
//                System.out.println(new String(back));
//                return h;
//            });


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

            HashSet<HGPersistentHandle> incidenceSet = new HashSet<HGPersistentHandle>();
            for (int i = 0; i < 5_000; i++)
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


            checkIncidence(linkH, incidenceSet, store);

            HGPersistentHandle removed = incidenceSet.stream().skip(4).findFirst().get();
            HGPersistentHandle anotherRemoved = incidenceSet.stream().skip(2).findFirst().get();
            incidenceSet.remove(removed);
            incidenceSet.remove(anotherRemoved);
            store.getTransactionManager().ensureTransaction(() -> {
                storageImpl.removeIncidenceLink(linkH, removed);
                storageImpl.removeIncidenceLink(linkH, anotherRemoved);
                return null;
            });

            checkIncidence(linkH, incidenceSet, store);

            store.getTransactionManager().ensureTransaction(() -> {
                storageImpl.removeIncidenceSet(linkH);
                return null;
            });

            if (store.getTransactionManager().ensureTransaction(() -> storageImpl.getIncidenceSetCardinality(linkH))
                    != 0)
                throw new RuntimeException("Incience set for " + linkH + " should be clear.");

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


}
