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
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;

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



    private TransactionDB datadb;
//    private RocksDB primitivedb;
//    private RocksDB incidencedb;

    private Options options;
    private TransactionDBOptions txoptions;
    private Filter bloomfilter;
    private ReadOptions readOptions;
    private Statistics stats;
    private RateLimiter rateLimiter;
    private HGStore store;
    private HGConfiguration hgConfig;
    private int handleSize;

    private enum KeyType
    {
        INCIDENCE("incidence"),
        PRIMITIVE("primitive"),
        DATA("data");

        private final byte[] prefix;
        private KeyType(String prefix)
        {
            this.prefix = prefix.getBytes(StandardCharsets.UTF_8);
        }

        public byte[] scopeKey(byte[] key)
        {
            byte[] res = new byte[prefix.length + key.length];
            System.arraycopy(prefix, 0, res, 0, prefix.length);
            System.arraycopy(key, 0, res, prefix.length, key.length);
            return res;
        }
    }

    @Override
    public Object getConfiguration()
    {
        return null;
    }

    private void setup()
    {
        /*
        All of the options below are from the sample application;
        TODO investigate what they do and verify if they  are correct
        TODO move to a config object
         */
        this.options = new Options();
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

        options.setMemTableConfig(
                new HashSkipListMemTableConfig().setHeight(4)
                        .setBranchingFactor(4).setBucketCount(2000000));
        options.setMemTableConfig(
                new HashLinkedListMemTableConfig().setBucketCount(100000));
        options.setMemTableConfig(
                new VectorMemTableConfig().setReservedSize(10000));
        options.setMemTableConfig(new SkipListMemTableConfig());
        options.setTableFormatConfig(new PlainTableConfig());
        options.setAllowMmapReads(true);
        options.setRateLimiter(rateLimiter);

        final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
        Cache cache = new LRUCache(64 * 1024, 6);
        table_options.setBlockCache(cache).setNoBlockCache(false)
                .setFilterPolicy(bloomfilter).setBlockSizeDeviation(5)
                .setBlockRestartInterval(10)
                .setCacheIndexAndFilterBlocks(true);

        options.setTableFormatConfig(table_options);

    }

    @Override
    public void shutdown()
    {
        this.options.close();
        this.txoptions.close();
        this.bloomfilter.close();
        this.readOptions.close();
        this.stats.close();
        this.rateLimiter.close();
        this.datadb.close();
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
            this.datadb = TransactionDB.open(options, txoptions, Path.of(store.getDatabaseLocation()).toString());
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
                     */
                    /*
                    TODO how do we set the transaction as readonly?
                     */
                    final TransactionOptions txnOptions = new TransactionOptions().setSetSnapshot(true);
                    final WriteOptions writeOptions = new WriteOptions();
//                    var parentTxn = ((StorageTransactionRocksDB)parent.getStorageTransaction()).txn();
                    /*
                    TODO do we have the correct semantics for the parent transaction?


                    TODO in RocksDB the transaction is created in the
                     */
                    final Transaction txn = datadb.beginTransaction(writeOptions, txnOptions);
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
        return tx == null ?
                StorageTransactionRocksDB.nullTransaction() :
                (StorageTransactionRocksDB) tx.getStorageTransaction();
    }


    @Override
    public HGPersistentHandle store(HGPersistentHandle handle,
            HGPersistentHandle[] link)
    {
        var key = handle.toByteArray();
        var value = fromHandleArray(link);
        try
        {
            txn().rocksdbTxn().put(KeyType.DATA.scopeKey(key), value);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }

        return handle;

    }

    @Override
    public HGPersistentHandle store(HGPersistentHandle handle, byte[] data)
    {
        try
        {
            txn().rocksdbTxn().put(KeyType.PRIMITIVE.scopeKey(handle.toByteArray()), data);
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
            return txn().rocksdbTxn().get(readOptions, KeyType.PRIMITIVE.scopeKey(handle.toByteArray()));
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
            byte[] bytes = txn().rocksdbTxn().get(readOptions, KeyType.DATA.scopeKey(handle.toByteArray()));
            return toHandleArray(bytes);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }

    private void removeKey(byte[] key)
    {
        try
        {
            txn().rocksdbTxn().delete(key);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }

    @Override
    public void removeData(HGPersistentHandle handle)
    {
        this.removeKey(KeyType.PRIMITIVE.scopeKey(handle.toByteArray()));
    }


    @Override
    public void removeLink(HGPersistentHandle handle)
    {
        this.removeKey(KeyType.DATA.scopeKey(handle.toByteArray()));
    }

    private boolean containsKey(byte[] key)
    {
        try
        {
            return txn().rocksdbTxn().get(readOptions, key) != null;
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }

    }

    @Override
    public boolean containsData(HGPersistentHandle handle)
    {
        return containsKey(KeyType.PRIMITIVE.scopeKey(handle.toByteArray()));
    }

    @Override
    public boolean containsLink(HGPersistentHandle handle)
    {
        return containsKey(KeyType.DATA.scopeKey(handle.toByteArray()));
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
        return null;
    }

    @Override
    public void removeIncidenceSet(HGPersistentHandle handle)
    {

    }

    @Override
    public long getIncidenceSetCardinality(HGPersistentHandle handle)
    {
        return 0;
    }

    @Override
    public void addIncidenceLink(HGPersistentHandle handle,
            HGPersistentHandle newLink)
    {

    }

    @Override
    public void removeIncidenceLink(HGPersistentHandle handle,
            HGPersistentHandle oldLink)
    {

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

    static void checkIncidence(HGPersistentHandle atom, HashSet<HGPersistentHandle> incidenceSet, HGStore store)
    {
        store.getTransactionManager().ensureTransaction(() -> {
            long storedIncidentCount = store.getIncidenceSetCardinality(atom);

            if (storedIncidentCount != incidenceSet.size())
                throw new RuntimeException("Not same number of incident links,  " + storedIncidentCount +
                        ", expecting " + incidenceSet.size());

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
            HGPersistentHandle h = config.getHandleFactory().makeHandle();

            store.getTransactionManager().ensureTransaction(() -> {
                storageImpl.store(h, "Hello world".getBytes());
                byte [] back = storageImpl.getData(h);
                System.out.println(new String(back));
                return h;
            });

            storageImpl.shutdown();
            storageImpl.startup(store, config);

            store.getTransactionManager().ensureTransaction(() -> {
                byte [] back = storageImpl.getData(h);
                System.out.println(new String(back));
                return h;
            });


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

            if (1==1)
            {
                System.out.println("CUT SHORT");
                return;
            }

            HashSet<HGPersistentHandle> incidenceSet = new HashSet<HGPersistentHandle>();
            for (int i = 0; i < 11; i++)
                incidenceSet.add(config.getHandleFactory().makeHandle());

            store.getTransactionManager().ensureTransaction(() -> {
                for (HGPersistentHandle incident : incidenceSet)
                    storageImpl.addIncidenceLink(linkH, incident);
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
