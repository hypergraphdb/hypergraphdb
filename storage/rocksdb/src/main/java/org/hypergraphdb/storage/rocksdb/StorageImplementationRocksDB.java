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
import org.hypergraphdb.storage.rocksdb.dataformat.FixedKeyFixedValueColumnFamilyMultivaluedDB;
import org.hypergraphdb.storage.rocksdb.index.BidirectionalRocksDBIndex;
import org.hypergraphdb.storage.rocksdb.index.HGIndexAdapter;
import org.hypergraphdb.storage.rocksdb.index.RocksDBIndex;
import org.hypergraphdb.transaction.*;
import org.hypergraphdb.util.HGUtils;
import org.rocksdb.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageImplementationRocksDB implements HGStoreImplementation
{

    /*
    TODO when does this need to be loaded?
     */
    static {
        RocksDB.loadLibrary();
    }

    private OptimisticTransactionDB db;

   /*
    Mostly use the defaults for DBptions and column family options.
    This is the recommended approach for starters.
    */
    private final DBOptions dbOptions = new DBOptions()
           .setCreateMissingColumnFamilies(true)
           .setCreateIfMissing(true);
    /*
    Stores the column family options used for each column family so that we
    can close them when we close the storage
     */
    private final List<ColumnFamilyOptions> cfOptions = new ArrayList<>();


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
    private boolean started = false;

    private void checkStarted()
    {
        if (!started)
            throw new HGException("The storage layer is not started");
    }


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
    private List<ColumnFamilyDescriptor> getColumnFamilyDescriptors() throws RocksDBException
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

        for (var cfID : parsedColumnFamilyIDs)
        {
            ColumnFamilyDescriptor cfd;

            if (isIndex(cfID) || isInverseIndex(cfID))
            {
                /*
                if we are opening a column family which represents
                either an index or inverse index, we want to set
                a custom comparator.
                The way we do that is to pass the RocksDB runtime
                a reference to a HGIndexAdapter.getComparator().
                Later when the user opens the index, they will pass
                the actual comparator and we can setup the HGIndexAdapter
                 */
                var indexName = stripCFPrefix(cfID);
                if (!this.indexAdapters.containsKey(indexName))
                {
                    HGIndexAdapter adapter = new HGIndexAdapter(indexName);
                    this.indexAdapters.put(indexName, adapter);
                }
                var cfOptions = new ColumnFamilyOptions()
                        .setComparator(this.indexAdapters.get(indexName).getRocksDBComparator());
                this.cfOptions.add(cfOptions);
                cfd = new ColumnFamilyDescriptor(cfID.getBytes(StandardCharsets.UTF_8), cfOptions);

            }
            else
            {
                var cfOptions = new ColumnFamilyOptions();
                this.cfOptions.add(cfOptions);
                cfd = new ColumnFamilyDescriptor(cfID.getBytes(), cfOptions);
            }
            descriptors.add(cfd);
        }

        /*
        Add the non index column families which are not already present
        in the DB.
        Note that when we are specifying the column families, we need to
        specify the default explicitly
        Opening the DB will create them.
         */
        Stream.of("default", CF_DATA, CF_PRIMITIVE, CF_INCIDENCE)
            .filter(Predicate.not(parsedColumnFamilyIDs::contains))
            .forEach(cf ->{
                var cfOptions = new ColumnFamilyOptions();
                this.cfOptions.add(cfOptions);
                descriptors.add(new ColumnFamilyDescriptor(cf.getBytes(StandardCharsets.UTF_8)));
            });

        return descriptors;

    }

    private final ConcurrentHashMap<String, RocksDBIndex<?,?>> indices = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, HGIndexAdapter> indexAdapters = new ConcurrentHashMap<>();

    /**
     * whether a column family is an index
     * @param cfName the column family to check
     * @return true iff the parameter starts with the prefix we expect for a column family which
     * contains an index
     */
    private static boolean isIndex(String cfName)
    {
        return cfName.startsWith(CF_INDEX_PREFIX);
    }

     /**
     * whether a column family is an inverse index
     * @param cfName the column family to check
     * @return true iff the parameter starts with the prefix we expect for a column family
     * which contains an inverse index
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


    @Override
    public void shutdown()
    {
        this.started = false;
        this.db.close();
        this.dbOptions.close();
        for (ColumnFamilyOptions cfOption : this.cfOptions)
        {
            cfOption.close();
        }
    }


    /*
    TODO use the supplied configuration to set the needed options;
        determine which options are needed -- Options, DBOptions, ReadOptions,
        TransactionOptions etc.
     */
    @Override
    public void startup(HGStore store, HGConfiguration configuration)
    {
        /*
        A single storage must be started only once. What are the actions which
        should not be performed more than once?
        TODO when do we set the flag
         */
        this.started = true;

        this.store = store;
        this.hgConfig = configuration;
        this.handleSize = configuration.getHandleFactory().nullHandle().toByteArray().length;
        this.columnFamilies = new ConcurrentHashMap<>();

        try
        {
            /*
            Get the column family descriptors of the column families
            which are already present in the database
             */
            List<ColumnFamilyDescriptor> cfDescriptors = getColumnFamilyDescriptors();

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

            for (int i = 0; i < cfDescriptors.size(); i++)
            {
                columnFamilies.putIfAbsent(
                        new String(cfDescriptors.get(i).getName(), StandardCharsets.UTF_8),
                        cfHandles.get(i));
            }
            /*
            If this is a brand new instance, there will be no column
            families in it, so
             */
//            ensureColumnFamilies(cfDescriptors, cfHandles);
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
                    final WriteOptions writeOptions = new WriteOptions();
                    Transaction parentTxn = null;
                    if (parent != null)
                    {
                        parentTxn = ((RocksDBStorageTransaction)parent.getStorageTransaction()).rocksdbTxn();
                    }
                    /*
                    TODO do we have the correct semantics for the parent transaction?
                    TODO in RocksDB the transaction is created in the
                     */;
                    Transaction txn;

                    /*
                    Set a snapshot to the tx when the transaction begins
                    This will be the snapshot will be the initial state
                    the transaction  sees (by default, each record will
                    have the state before it was first written to)
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
        /*
        TODO
            this will fail. The client code should check whether the null transaction
            is active
         */
        if (tx == null)
        {
            return RocksDBStorageTransaction.nullTransaction();
        }
        else if (tx.getStorageTransaction() instanceof VanillaTransaction)
        {
            /*
             TODO what should the client do
                when the factory is creating VanillaTransactions?
            ???
             */
            return null;
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
    public HGPersistentHandle store(
            HGPersistentHandle handle,
            HGPersistentHandle[] link)
    {
        checkStarted();
        var key = handle.toByteArray();
        var value = fromHandleArray(link);
        this.ensureTransaction(tx -> {
           try
           {
               tx.put(columnFamilies.get(CF_DATA), key, value);
               return null;
           }
           catch (RocksDBException e)
           {
               throw new HGException(e);
           }
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
            try
            {
                tx.put(
                        columnFamilies.get(CF_PRIMITIVE),
                        handle.toByteArray(),
                        data);
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }

        });
        return handle;
    }

    @Override
    public byte[] getData(HGPersistentHandle handle)
    {
        checkStarted();
        return ensureTransaction(tx -> {
            try (ReadOptions readOptions = new ReadOptions())
            {
                /*
                read from the snapshot set on the transaction
                 */
                readOptions.setSnapshot(tx.getSnapshot());
                var res = tx.get(
                        columnFamilies.get(CF_PRIMITIVE),
                        readOptions,
                        handle.toByteArray());

                return res;
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }
        });
    }

    @Override
    public HGPersistentHandle[] getLink(HGPersistentHandle handle)
    {
        checkStarted();
        return ensureTransaction(tx -> {
            try (ReadOptions readOptions = new ReadOptions())
            {
                readOptions.setSnapshot(tx.getSnapshot());
                byte[] bytes = tx.get(columnFamilies.get(CF_DATA), readOptions, handle.toByteArray());

                return bytes == null ? null : toHandleArray(bytes);
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }
        });
    }


    @Override
    public void removeData(HGPersistentHandle handle)
    {
        checkStarted();
        ensureTransaction(tx -> {
            try
            {
                tx.delete(
                        columnFamilies.get(CF_PRIMITIVE),
                        handle.toByteArray());
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }

        });
    }


    @Override
    public void removeLink(HGPersistentHandle handle)
    {
        checkStarted();
        ensureTransaction(tx -> {
            try
            {
                tx.delete(
                        columnFamilies.get(CF_DATA),
                        handle.toByteArray());
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }
        });
    }


    @Override
    public boolean containsData(HGPersistentHandle handle)
    {
        checkStarted();
        return ensureTransaction(tx -> {
            try(var readOptions = new ReadOptions())
            {
                readOptions.setSnapshot(tx.getSnapshot());
                return tx.get(
                        columnFamilies.get(CF_PRIMITIVE),
                        readOptions,
                        handle.toByteArray()) != null;
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }
        });
    }

    @Override
    public boolean containsLink(HGPersistentHandle handle)
    {
        checkStarted();
        return ensureTransaction(tx -> {
            try(var readOptions = new ReadOptions())
            {
                readOptions.setSnapshot(tx.getSnapshot());
                return tx.get(
                        columnFamilies.get(CF_DATA),
                        readOptions,
                        handle.toByteArray() ) != null;
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }
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
            throw new HGException(String.format("Cannot convert a buffer of size %s to a handle. The expected size is %s", buffer.length, handleSize));
        }

        return this.hgConfig.getHandleFactory().makeHandle(buffer);
    }


    @Override
    public HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(
            HGPersistentHandle handle)
    {
        checkStarted();
        return ensureTransaction(tx -> {
            return new IteratorResultSet<HGPersistentHandle>(
                /*
                TODO ReadOptions, Slice object close
                 */
                    tx.getIterator(new ReadOptions()
                                    .setSnapshot(tx.getSnapshot())
                                    .setIterateLowerBound(
                                            new Slice(
                                                    FixedKeyFixedValueColumnFamilyMultivaluedDB.firstRocksDBKey(handle.toByteArray())))
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
        });


    }



    @Override
    public void removeIncidenceSet(HGPersistentHandle handle)
    {
        /*
        TODO consider range delete instead of iterating over the entire
            index
         */
        checkStarted();
        ensureTransaction(tx -> {
            try (
                    var first = new Slice(
                            FixedKeyFixedValueColumnFamilyMultivaluedDB.firstRocksDBKey(handle.toByteArray()));
                    var last = new Slice(
                            FixedKeyFixedValueColumnFamilyMultivaluedDB.lastRocksDBKey(handle.toByteArray()));

                    var iteratorReadOptions = new ReadOptions()
                            .setIterateLowerBound(first)
                            .setIterateUpperBound(last)
                            .setSnapshot(tx.getSnapshot());

                    RocksIterator iterator = tx.getIterator(
                            iteratorReadOptions,
                            columnFamilies.get(CF_INCIDENCE)))
            {
                while (iterator.isValid())
                {
                    iterator.next();
                    byte[] next = iterator.key();
                    try
                    {
                        tx.delete(columnFamilies.get(CF_INCIDENCE), next);
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

            var rocksDBkey = FixedKeyFixedValueColumnFamilyMultivaluedDB.makeRocksDBKey(localKey, value);
            try
            {
                tx.put(columnFamilies.get(CF_INCIDENCE), rocksDBkey, new byte[0]);
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }

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
            var rocksDBkey = FixedKeyFixedValueColumnFamilyMultivaluedDB.makeRocksDBKey(localKey, value);
            try
            {
                tx.delete(columnFamilies.get(CF_INCIDENCE), rocksDBkey);
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }
        });

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
        checkStarted();
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
        checkStarted();

        RocksDBIndex<KeyType, ValueType> index = (RocksDBIndex<KeyType, ValueType>) indices.get(name);
        if (index != null)
        {
            return index;
        }

        ColumnFamilyHandle cfHandle = null, inverseCFHandle = null;
        var adapter = indexAdapters.get(name);
        if (adapter == null)
        {
            /*
             if the adapter is null, the column families were not present
             at startup, so we need to create them now.
             TODO
                what about just one if bidirectional?
            */
            adapter = new HGIndexAdapter(name);
            indexAdapters.put(name, adapter);
            /*
            TODO lifecycle of the column family options
             */
            ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(
                    indexCF(name).getBytes(StandardCharsets.UTF_8),
                    new ColumnFamilyOptions().setComparator(adapter.getRocksDBComparator()));
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
                        new ColumnFamilyOptions().setComparator(adapter.getRocksDBComparator()));
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
                    keyConverter,
                    valueConverter,
                    db,
                    this);
        }
        else
        {
            index = new RocksDBIndex<KeyType, ValueType>(
                    name,
                    cfHandle,
//                    this.store.getTransactionManager(),
                    keyConverter,
                    valueConverter,
                    db,
                    this);
        }

        /*
        Register the supplied comparators to the adapter so that the
        comparators are available for index comparison
         */
        adapter.configure(keyComparator, valueComparator);
        indices.put(name, index);

        return index;
    }

    @Override
    public void removeIndex(String name)
    {
        checkStarted();
        //Delete the entire column family
        var cf = columnFamilies.get(indexCF(name));

        if (cf == null)
        {
            throw new HGException(String.format(
                    "Cannot remove index %s whose column family - %s does not exist.",
                    name, indexCF(name)));
        }
        else
        {
            try
            {
                db.dropColumnFamily(cf);
                columnFamilies.remove(cf);
            }
            catch (RocksDBException e)
            {
                throw new HGException(
                        String.format("Could not delete column family %s which stores" +
                                "the index %s.", indexCF(name), name), e);
            }
        }
        var inversecf = columnFamilies.get(inverseIndexCF(name));

        if (inversecf != null)
        {
            try
            {
                db.dropColumnFamily(inversecf);
                columnFamilies.remove(inversecf);
            }
            catch (RocksDBException e)
            {
                throw new HGException(
                        String.format("Could not delete column family %s which stores" +
                                "the index %s.", inverseIndexCF(name), name), e);
            }
        }

        indexAdapters.remove(name);
        indices.remove(name);

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

    /**
     * Execute a function in a transaction. If there is a current transaction,
     * the function will be executed in it.
     * If there is no current transaction, a new one just for the operation will be
     * created unless the enforce transactions config option is set.
     *
     * @param f the function to execute.
     * @return
     * @param <T>
     */
    public <T> T ensureTransaction(Function<Transaction, T> f)
    {
        var currentTxn = txn();
        if (currentTxn.rocksdbTxn() != null)
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


    public <T> void ensureTransaction(Consumer<Transaction> f)
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
