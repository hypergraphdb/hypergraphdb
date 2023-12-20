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

    /**
     * Guards the state of the indices
     */
    private final Object indexLock = new Object();

    private volatile boolean started = false;
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

    //column family name -- column family options
    private final ConcurrentHashMap<String, ColumnFamilyOptions> cfOptionsStore = new ConcurrentHashMap<>();
    //column family name -- column family handle
    private final ConcurrentHashMap<String, ColumnFamilyHandle> columnFamilies = new ConcurrentHashMap<>();
    //index name -- index
    private final ConcurrentHashMap<String, RocksDBIndex<?,?>> indices = new ConcurrentHashMap<>();
    //index name -- index adapter
    private final ConcurrentHashMap<String, HGIndexAdapter> indexAdapters = new ConcurrentHashMap<>();


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
     * @return
     * @throws RocksDBException
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
                this.cfOptionsStore.put(cfID, cfOptions);
                cfd = new ColumnFamilyDescriptor(cfID.getBytes(StandardCharsets.UTF_8), cfOptions);

            }
            else
            {
                var cfOptions = new ColumnFamilyOptions();
                this.cfOptionsStore.put(cfID, cfOptions);
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
            .forEach(cfID -> {
                var cfOptions = new ColumnFamilyOptions();
                this.cfOptionsStore.put(cfID, cfOptions);
                descriptors.add(new ColumnFamilyDescriptor(cfID.getBytes(StandardCharsets.UTF_8)));
            });

        return descriptors;

    }


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
            throw new IllegalArgumentException(String.format("%s is not an index column family", cfName));
        }
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

            for (int i = 0; i < cfDescriptors.size(); i++)
            {
                columnFamilies.putIfAbsent(
                        new String(cfDescriptors.get(i).getName(), StandardCharsets.UTF_8),
                        cfHandles.get(i));
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
            throw new HGException(e);
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
        this.dbOptions.close();
        for (var index : this.indices.values())
        {
            index.close();
        }
        this.indices.clear();
        this.indexAdapters.clear();

        /*
        close the cf options used to initialize the column families
         */
        for (ColumnFamilyOptions cfOption : this.cfOptionsStore.values())
        {
            cfOption.close();
        }
        this.cfOptionsStore.clear();
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
            try (ReadOptions readOptions = new ReadOptions().setSnapshot(tx.getSnapshot()))
            {
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
            try (ReadOptions readOptions = new ReadOptions().setSnapshot(tx.getSnapshot()))
            {
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
            try(var readOptions = new ReadOptions().setSnapshot(tx.getSnapshot()))
            {
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
            try(var readOptions = new ReadOptions().setSnapshot(tx.getSnapshot()))
            {
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
            var lower =  new Slice(
                    FixedKeyFixedValueColumnFamilyMultivaluedDB.firstRocksDBKey(handle.toByteArray()));
            var upper = new Slice(FixedKeyFixedValueColumnFamilyMultivaluedDB.lastRocksDBKey(handle.toByteArray()));
            var ro = new ReadOptions()
                    .setSnapshot(tx.getSnapshot())
                    .setIterateLowerBound(lower)
                    .setIterateUpperBound(upper);

            return new IteratorResultSet<HGPersistentHandle>(
                    tx.getIterator(ro,
                            columnFamilies.get(CF_INCIDENCE)),
                    List.of(lower, upper, ro),
                    false)

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
            index when it is implemented for transctions in java rocks db
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

        synchronized (this.indexLock)
        {
            if (index != null)
            {
                return index;
            }
            ColumnFamilyHandle cfHandle, inverseCFHandle = null;
            var adapter = indexAdapters.get(name);
            if (adapter == null)
            {
            /*
             if the adapter is null, the column family(s) were not present
             at startup, which means the index is not present in the db,
             so we need to create them now.
            */
                if (cfOptionsStore.containsKey(indexCF(name)) || columnFamilies.containsKey(indexCF(name)))
                {
                    throw new HGException(String.format("This is probably a bug. There was no index adapter for an index" +
                                    " %s but a column family for that index appears to be setup. CF Options for name %s exists: %s. CF " +
                                    " for name %s exists: %s",
                            name,
                            indexCF(name),
                            cfOptionsStore.containsKey(indexCF(name)),
                            indexCF(name),
                            columnFamilies.containsKey(indexCF(name))));
                }

                adapter = new HGIndexAdapter(name);
                indexAdapters.put(name, adapter);
                var cfo = new ColumnFamilyOptions().setComparator(adapter.getRocksDBComparator());
                cfOptionsStore.put(indexCF(name), cfo);
                var cfd = new ColumnFamilyDescriptor(indexCF(name).getBytes(StandardCharsets.UTF_8), cfo);

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
                /*
                 if the adapter is null, the column family(s) were not present
                 at startup, which means the index is not present in the db,
                 so we need to create them now.
                */
                    if (cfOptionsStore.containsKey(inverseIndexCF(name)) || columnFamilies.containsKey(inverseIndexCF(name)))
                    {
                        throw new HGException(String.format("This is probably a bug. There was no index adapter for an index" +
                                        " %s but a column family for the inverse index appears to be setup. CF Options for name %s exists: %s. CF " +
                                        " for name %s exists: %s",
                                name,
                                inverseIndexCF(name),
                                cfOptionsStore.containsKey(inverseIndexCF(name)),
                                inverseIndexCF(name),
                                columnFamilies.containsKey(inverseIndexCF(name))));
                    }
                    var invCFO = new ColumnFamilyOptions().setComparator(adapter.getRocksDBComparator());
                    cfOptionsStore.put(inverseIndexCF(name), invCFO);
                    var inverseCFD = new ColumnFamilyDescriptor(inverseIndexCF(name).getBytes(StandardCharsets.UTF_8), invCFO);

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
            /*
            the index adapter is not null which means that this is a preexisting index which we have not
            yet initialized.
             */
                cfHandle = columnFamilies.get(indexCF(name));
                inverseCFHandle = columnFamilies.get(inverseIndexCF(name));
            }

            if (cfHandle == null || (isBidirectional && inverseCFHandle == null))
            {
            /*
            This is possible if not all the necessary column families were present at startup
            e.g. only the inverse column family or only the forward column family when the index is bidirectional
             */
                throw new HGException(String.format("Creating an index named %s. Its adapter " +
                        "is present and so its column families should be present as well. " +
                        "Bidirectional: %s.", name, isBidirectional));
            }
            if (!isBidirectional && inverseCFHandle != null)
            {
                throw new HGException(String.format("Creating a non bidirectional index named %s. " +
                                "A column family for the inverse index is present which is illegal. ",
                        name));
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
                        keyConverter,
                        valueConverter,
                        db,
                        this);
            }

        /*
        Register the supplied comparators with the adapter
         */
            adapter.configure(keyComparator, valueComparator);
            indices.put(name, index);

            return index;

        }
    }

    @Override
    public void removeIndex(String name)
    {
        checkStarted();
        synchronized (indexLock)
        {
            //Delete the entire column family
            var indexCFName = indexCF(name);
            var inverseIndexCFName = inverseIndexCF(name);

            var indexCFHandle = this.columnFamilies.get(indexCFName);
            var indexCFOptions = this.cfOptionsStore.remove(indexCFName);
            if (indexCFOptions != null)
            {
                indexCFOptions.close();
            }

            if (indexCFHandle == null)
            {
                throw new HGException(String.format(
                        "Cannot remove index %s whose column family - %s does not exist.",
                        name, indexCF(name)));
            }
            else
            {
                try
                {
                    db.dropColumnFamily(indexCFHandle);
                    columnFamilies.remove(indexCFName);
                }
                catch (RocksDBException e)
                {
                    throw new HGException(
                            String.format("Could not delete column family %s which stores" +
                                    "the index %s.", indexCF(name), name), e);
                }
            }
            var inverseIndexCFHandle = this.columnFamilies.get(inverseIndexCFName);

            var inverseIndexCFOptions = this.cfOptionsStore.remove(inverseIndexCFName);
            if (inverseIndexCFOptions != null)
            {
                inverseIndexCFOptions.close();
            }

            if (inverseIndexCFHandle != null)
            {
                try
                {
                    db.dropColumnFamily(inverseIndexCFHandle);
                    columnFamilies.remove(inverseIndexCFName);
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


}
