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
import java.util.concurrent.ConcurrentHashMap;

public class StorageImplementationRocksDB implements HGStoreImplementation
{

    //    private static final String DB_ROOT = "data";
//    private static final String DATA_DB_NAME = "datadb";
//    private static final String PRIMITIVE_DB_NAME = "primitivedb";
//    private static final String INCIDENCE_DB_NAME = "incidencedb";

    /*
    TODO when does this need to be loaded?
     */
    static {
        RocksDB.loadLibrary();
    }


    private TransactionDB db;

    private DBOptions dbOptions;
    private Options options;
    private TransactionDBOptions txDBoptions;
    private Filter bloomfilter;
    private ReadOptions readOptions;
    private Statistics stats;
    private RateLimiter rateLimiter;
    private HGStore store;
    private HGConfiguration hgConfig;
    private int handleSize;

    /*
     *TODO
     *  create an abstraction for a logical database, complete with
     *  the necessary  database operations
     */

    private static final String CF_INCIDENCE = "INCIDENCE";
    private static final String CF_DATA= "DATA";
    private static final String CF_PRIMITIVE = "PRIMITIVE";
    private static final String CF_INDEX_PREFIX = "INDEX_";
    private static final String CF_INVERSE_INDEX_PREFIX = "INV_INDEX_";

    /*
    TODO move the initialization logic
        The enum becomes a map
        the map is reset on Storage restart (startup())
     */
    private ConcurrentHashMap<String, ColumnFamilyHandle> columnFamilies;

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
     * Get all column family descriptors present in the database
     * This is called before the start of the DB because we need to
     * explicitly supply all the column families present in the
     * DB.
     * This is so that when the DB is opened, RocksDB knows about the
     * descriptor, namely the options which are not serialized but are needed
     * for a column family from the very beginning -- say comparators
     * @return
     * @throws RocksDBException
     */
    private List<ColumnFamilyDescriptor>getColumnFamilyDescriptors() throws RocksDBException
    {
        var columnFamilyIDs = TransactionDB.listColumnFamilies(
                options,
                Path.of(store.getDatabaseLocation()).toString());

        List<ColumnFamilyDescriptor> descriptors = new LinkedList<>();

        for (var cfID : columnFamilyIDs)
        {
            ColumnFamilyDescriptor cfd;

            String cfName = new String(cfID, StandardCharsets.UTF_8);

            if (isIndex(cfName) || isInverseIndex(cfName))
            {
                /*
                we are opening a column family for an index
                the difference is that we are setting a custom comparator
                how do we set the comparator?

                the compare method will call the

                The index is created when the database is started

                We need to supply th

                 */
                var indexName = stripCFPrefix(cfName);
                if (!this.indexAdapter.contains(indexName))
                {
                    HGIndexAdapter adapter = new HGIndexAdapter(stripCFPrefix(cfName));
                    this.indexAdapter.put(stripCFPrefix(cfName), adapter);
                }
                /*
                The cfd needs a comparator function
                the comparator needs a reference to the index (which we do not
                currently have)
                so we supply the adapter's comparator which now is just
                scaffolding, and will be hydrated when the user calles getIndex()
                 */
                cfd = new ColumnFamilyDescriptor(cfID,
                        new ColumnFamilyOptions().setComparator(this.indexAdapter.get(indexName).getComparator()));

            }
            else
            {
                cfd = new ColumnFamilyDescriptor(cfID, new ColumnFamilyOptions());
            }
            descriptors.add(cfd);
        }


        if (descriptors.isEmpty())
            descriptors.add(new ColumnFamilyDescriptor("default".getBytes(
                    StandardCharsets.UTF_8)));

        return descriptors;

    }

    private ConcurrentHashMap<String, RocksDBIndex> indices = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, HGIndexAdapter> indexAdapter = new ConcurrentHashMap<>();

    /**
     * whether a column family is an index
     * @param cfName
     * @return
     */
    private static boolean isIndex(String  cfName)
    {
        return cfName.startsWith(CF_INDEX_PREFIX);
    }

    /**
     * whether a column family is an inverse index
     * @param cfName
     * @return
     */
    private static boolean isInverseIndex(String  cfName)
    {
        return cfName.startsWith(CF_INVERSE_INDEX_PREFIX);
    }

    private static String stripCFPrefix(String cfName)
    {
        if (isIndex(cfName))
        {
           return cfName.substring(CF_INDEX_PREFIX.length());
        }
        else if (isInverseIndex(cfName))
        {
            return cfName.substring(CF_INVERSE_INDEX_PREFIX.length());
        }
        else
        {
            throw new IllegalArgumentException(String.format("%s is not an index" +
                    "column family", cfName));
        }
    }


    /**
     * configure all the options objects needed for starting the database
     */
    private void setup()
    {
        /*
        All of the options below are from the sample application;
        TODO investigate what they do and verify if they  are correct
        TODO move to a config object
         */
        this.options = new Options();
        this.dbOptions = new DBOptions().setCreateIfMissing(true);
        this.txDBoptions = new TransactionDBOptions();
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
        this.txDBoptions.close();
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
        this.columnFamilies = new ConcurrentHashMap<>();

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

            The
             */
            List<ColumnFamilyDescriptor> cfDescriptors = getColumnFamilyDescriptors();

            /*
            open() needs the present column family descriptors. it will then
            populate the cfHandles parameter with the handles corresponfing
            to these column families.
            The call to open will fill this with the column family handles

            What happens with column families which are supplied, but do
            not exist

            What happens with column families which exist, but are not supplied?

            dbOptions.setCreateMissing

            how is the default column family handled
             */
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

            this.db = TransactionDB.open(
                    dbOptions,
//                    dbOptions.setCreateMissingColumnFamilies(),
                    txDBoptions,
                    Path.of(store.getDatabaseLocation()).toString(),
                    cfDescriptors,
                    cfHandles);

            /*
            Create the column families which do not exist.

             */
            ensureColumnFamilies(cfDescriptors, cfHandles);
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
     */
    private void ensureColumnFamilies(
            List<ColumnFamilyDescriptor> cfDescriptors,
            List<ColumnFamilyHandle> cfHandles) throws RocksDBException
    {
        /*
        We assume the column families are initialized once in a program run
        which is not a valid assumption.
         */
        if (cfHandles.size() != cfDescriptors.size())
        {
            throw new RuntimeException("The sizes have to be the same");
        }
        /*
        populate the column families map
         */
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

            columnFamilies.putIfAbsent(cfName, handle);

        }
        /*
        if some column families are not initialized from the existing
        database, create them
         */
        if (!columnFamilies.containsKey(CF_PRIMITIVE))
        {
            /*
            The keys in the primitive database are the serialization
            of the handles of the first order atoms.
            They can be compared lexicographically so no need to set a custom comparator
             */
            ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(
                    (CF_PRIMITIVE.getBytes(StandardCharsets.UTF_8)));
            var handle = db.createColumnFamily(descriptor);
            columnFamilies.put(CF_PRIMITIVE, handle);
        }

        if (!columnFamilies.containsKey(CF_DATA))
        {
            /*
            The keys in the primitive database are the serialization
            of the handles of the first order atoms.
            They can be compared lexicographically so no need to set a custom comparator
             */
            ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(
                    (CF_DATA.getBytes(StandardCharsets.UTF_8)));
            var handle = db.createColumnFamily(descriptor);
            columnFamilies.put(CF_DATA, handle);
        }

        if (!columnFamilies.containsKey(CF_INCIDENCE))
        {
            /*
            The keys in the primitive database are the serialization
            of the handles of the first order atoms.
            They can be compared lexicographically so no need to set a custom comparator
             */
            ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(
                    (CF_INCIDENCE.getBytes(StandardCharsets.UTF_8)));
            var handle = db.createColumnFamily(descriptor);
            columnFamilies.put(CF_INCIDENCE, handle);
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
        /*
        TODO
            this will fail. The client code should check whether the null transaction
            is active
         */
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
            txn().rocksdbTxn().put(columnFamilies.get(CF_DATA), key, value);
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
            txn().rocksdbTxn().put(
                    columnFamilies.get(CF_PRIMITIVE),
                    handle.toByteArray(),
                    data);
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
            var res = txn().rocksdbTxn().get(
                    columnFamilies.get(CF_PRIMITIVE),
                    readOptions,
                    handle.toByteArray());

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

//            datadb.getA
            return res;
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }

    @Override
    public HGPersistentHandle[] getLink(HGPersistentHandle handle)
    {
        try
        {
            byte[] bytes = txn().rocksdbTxn().get(columnFamilies.get(CF_DATA), readOptions, handle.toByteArray());
            return toHandleArray(bytes);
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
            txn().rocksdbTxn().delete(
                    columnFamilies.get(CF_PRIMITIVE),
                    handle.toByteArray());
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
            txn().rocksdbTxn().delete(
                    columnFamilies.get(CF_DATA),
                    handle.toByteArray());
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
                    columnFamilies.get(CF_PRIMITIVE),
                    readOptions,
                    handle.toByteArray()) != null;
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
                    columnFamilies.get(CF_DATA),
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
        return new IteratorResultSet<HGPersistentHandle>(
                /*
                TODO ReadOptions, Slice object close
                 */
                txn().rocksdbTxn().getIterator(new ReadOptions()
                        .setIterateLowerBound(
                                new Slice(FixedKeyFixedValueColumnFamilyMultivaluedDB.firstRocksDBKey(handle.toByteArray())))
                        .setIterateUpperBound(
                                new Slice(FixedKeyFixedValueColumnFamilyMultivaluedDB.lastRocksDBKey(handle.toByteArray()))),
                columnFamilies.get(CF_INCIDENCE)), false)
        {
            @Override
            protected HGPersistentHandle extractValue()
            {
                return hgConfig.getHandleFactory().makeHandle(
                        FixedKeyFixedValueColumnFamilyMultivaluedDB.extractValue(
                                this.iterator.key()));
            }

            @Override
            protected byte[] toRocksDBKey(HGPersistentHandle value)
            {
                return FixedKeyFixedValueColumnFamilyMultivaluedDB.makeRocksDBKey(handle.toByteArray(), value.toByteArray());
            }
        };

    }



    @Override
    public void removeIncidenceSet(HGPersistentHandle handle)
    {
        try (
            var first = new Slice(
                    FixedKeyFixedValueColumnFamilyMultivaluedDB.firstRocksDBKey(handle.toByteArray()));
            var last = new Slice(
                    FixedKeyFixedValueColumnFamilyMultivaluedDB.lastRocksDBKey(handle.toByteArray()));

            var iteratorReadOptions = new ReadOptions().setIterateLowerBound(first).setIterateUpperBound(last);

            var iterator = txn().rocksdbTxn().getIterator(
                    iteratorReadOptions,
                    columnFamilies.get(CF_INCIDENCE)))
        {
            while (iterator.isValid())
            {
                iterator.next();
                byte[] next = iterator.key();
                try
                {
                    txn().rocksdbTxn().delete(columnFamilies.get(CF_INCIDENCE), next);
                }
                catch (RocksDBException e)
                {
                    throw new HGException(e);
                }
            }
            try
            {
                iterator.status();
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }

        }

    }

    @Override
    public long getIncidenceSetCardinality(HGPersistentHandle handle)
    {
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
        var localKey = atomHandle.toByteArray();
        var value = incidentLink.toByteArray();

        var rocksDBkey = FixedKeyFixedValueColumnFamilyMultivaluedDB.makeRocksDBKey(localKey, value);
        try
        {
            txn().rocksdbTxn().put(columnFamilies.get(CF_INCIDENCE), rocksDBkey, new byte[0]);
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
        var rocksDBkey = FixedKeyFixedValueColumnFamilyMultivaluedDB.makeRocksDBKey(localKey, value);
        try
        {
            txn().rocksdbTxn().delete(columnFamilies.get(CF_INCIDENCE), rocksDBkey);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }

    }

    /*
    for debugging only
     */
    static void checkIncidence(HGPersistentHandle atom, HashSet<HGPersistentHandle> incidenceSet, HGStore store)
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

    @Override
    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(
            String name)
    {
        return (RocksDBIndex<KeyType, ValueType>)indices.get(name);
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
        /*
        1. Create a new column family for that index.
        2. we need to store multiple values per key.
            both keys and values are varlength.
            how do we

         */

        HGIndex<KeyType, ValueType> index = indices.get(name);
        if (index != null)
        {
            return index;
        }

        ColumnFamilyHandle cfHandle = null, inverseCFHandle = null;
        var adapter = indexAdapter.get(name);
        if (adapter == null)
        {
            /*
             if the adapter is null, the column families were not present
             at startup, so we need to create them now.
             TODO
                what about just one if bidirectional?
            */
            adapter = new HGIndexAdapter(name);
            indexAdapter.put(name, adapter);
            /*
            TODO lifecycle of the column family options
             */
            ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(
                    indexCF(name).getBytes(StandardCharsets.UTF_8),
                    new ColumnFamilyOptions().setComparator(adapter.getComparator()));
            try
            {
                cfHandle = db.createColumnFamily(cfd);
                columnFamilies.put(indexCF(name), cfHandle);
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }
            if (isBidirectional)
            {
                ColumnFamilyDescriptor inverseCFD = new ColumnFamilyDescriptor(
                        inverseIndexCF(name).getBytes(StandardCharsets.UTF_8),
                        new ColumnFamilyOptions().setComparator(adapter.getComparator()));
                try
                {
                    inverseCFHandle = db.createColumnFamily(inverseCFD);
                }
                catch (RocksDBException e)
                {
                    throw new HGException(e);
                }
                columnFamilies.put(inverseIndexCF(name), inverseCFHandle);
            }
        }
        else
        {
            cfHandle = columnFamilies.get(indexCF(name));
            inverseCFHandle = columnFamilies.get(inverseIndexCF(name));
        }


        if (cfHandle == null || (isBidirectional && inverseCFHandle == null))
        {
            throw new HGException(String.format("Requesting an index named %s. Its adapter " +
                    "is present and so its column families should be present ads well." +
                    "Bidirectional: %s.", name, isBidirectional));
        }
        if (isBidirectional)
        {
            index = new BidirectionalRocksDBIndex<KeyType, ValueType>(
                    name,
                    cfHandle,
                    inverseCFHandle,
                    this.store.getTransactionManager(),
                    keyConverter,
                    valueConverter,
                    db);
        }
        else
        {
            index = new RocksDBIndex<KeyType, ValueType>(
                    name,
                    cfHandle,
                    this.store.getTransactionManager(),
                    keyConverter,
                    valueConverter,
                    db);
        }

        /*
        Register the supplied comparators to the adapter so that the
        comparators are available for index comparison
         */
        adapter.configure(keyComparator, valueComparator);

        return index;
    }

    @Override
    public void removeIndex(String name)
    {
        //Delete the entire column family
        var cf = columnFamilies.get(indexCF(name));

        if (cf == null)
            throw new HGException(String.format(
                    "Cannot remove index %s whose column family - %s does not exist.",
                    name, indexCF(name)));

        try
        {
            db.dropColumnFamily(cf);
            columnFamilies.remove(indexCF(name));
        }
        catch (RocksDBException e)
        {
            throw new HGException(
                    String.format("Could not delete column family %s which stores" +
                            "the index %s.", indexCF(name), name), e);
        }
    }

    private String indexCF(String indexName)
    {
        return CF_INDEX_PREFIX + indexName;
    }

    /**
     * The name for the inverse index column family for a given column
     * @param indexName
     * @return
     */
    private String inverseIndexCF(String indexName)
    {
        return CF_INVERSE_INDEX_PREFIX + indexName;
    }

    /**
     * Just print the contents of the
     * primitive db for debugging purposes.
     *
     */
    private void printPrimitiveDB()
    {
        var i = this.db.newIterator(columnFamilies.get(CF_PRIMITIVE));
        i.seekToFirst();

        while (i.isValid())
        {
            var k = i.key();
            var v = i.value();
            System.out.println();
            var parserdKey = this.toHandle(
                    Arrays.copyOfRange(k, 0, 16));
            var parsedValue = new String(v);
            System.out.printf("primitive; key(%s); value(%s) %n", parserdKey, parsedValue);

            //primitive
            i.next();
        }
        try
        {
            i.status();
        }
        catch (RocksDBException e)
        {
            System.out.println("iterator failed with an exception");
            throw new RuntimeException(e);
        }
        System.out.println("iterated the database successfully");
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
            storageImpl.printPrimitiveDB();

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



            checkIncidence(linkH, incidenceSet, store);

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


}
