/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2024 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.index;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.rocksdb.ColumnFamilyRegistry;
import org.hypergraphdb.storage.rocksdb.StorageImplementationRocksDB;
import org.hypergraphdb.storage.rocksdb.dataformat.VarKeyVarValueColumnFamilyMultivaluedDB;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the creation/retrieval/disposal of the indices and the backing
 * RocksDB resources associated with them
 */
public class IndexManager implements AutoCloseable
{

    private static final String CF_INDEX_PREFIX = "INDEX";
    private static final String CF_INVERSE_INDEX_PREFIX = "INV_INDEX";
    private static final String CF_NAME_SEPARATOR = ">>>";


    /*
    Indices which are fully initialised
     */
    private final ConcurrentHashMap<String, RocksDBIndex<?,?>> indices = new ConcurrentHashMap<>();

    /*
    The column families which are preexisting in the database but the indices
    for them are not yet initialized
     */
    private final ColumnFamilyRegistry preexistingColumnFamilies = new ColumnFamilyRegistry();

    private final Object indexLock = new Object();
    private final StorageImplementationRocksDB store;
    private OptimisticTransactionDB db;

    public IndexManager(OptimisticTransactionDB db, StorageImplementationRocksDB store)
    {
        this.db = db;
        this.store = store;
    }


    /**
     * Whether the given column family contains an index (either forward or inverse)
     */
    public static boolean isIndexCF(String columnFamily)
    {
        return _isIndex(columnFamily) || _isInverseIndex(columnFamily);
    }

    /**
     * Whether a column family constains a forward index
     * @param cfName the column family to check
     * @return true iff the parameter starts with the prefix we expect for a column family which
     * contains an index
     */
    private static boolean _isIndex(String cfName)
    {
        return cfName.startsWith(CF_INDEX_PREFIX);
    }

    /**
     * Whether a column family contains an inverse index
     * @param cfName the column family to check
     * @return true iff the parameter starts with the prefix we expect for a column family
     * which contains an inverse index
     */
    private static boolean _isInverseIndex(String  cfName)
    {
        return cfName.startsWith(CF_INVERSE_INDEX_PREFIX);
    }

    /**
     * Construct the name for the column family which will contain a given index
     * @param indexName the name of the index
     * @param keyComparatorClass the class name of the comparator which is to be
     *                           used when comparing the keys in this index
     * @param valueComparatorClass the class name of the comparator which is to be
     *                           used when comparing the values in this index
     */
    private static String indexCF(String indexName, String keyComparatorClass, String valueComparatorClass)
    {
        return new StringJoiner(CF_NAME_SEPARATOR)
                .add(CF_INDEX_PREFIX)
                .add(indexName)
                .add(keyComparatorClass)
                .add(valueComparatorClass)
                .toString();
    }

    /**
     * Construct the name for the column family which will contain the inverse index
     * for a given bidirectional index
     * @param indexName the name of the index
     * @param keyComparatorClass the class name of the comparator which is to be
     *                           used when comparing the keys in this index
     *                           This is the comparator for the original keys in
     *                           the index, not the comparator for the values
     *                           which become keys in the inverse index
     * @param valueComparatorClass the class name of the comparator which is to be
     *                             used when comparing the values in this index.
     *                             This is the comparator for the original values
     *                             in the index, not the comparator for the keys
     *                             which become values in the inverse index.
     *
     */
    private static String inverseIndexCF(String indexName, String keyComparatorClass, String valueComparatorClass)
    {
        return new StringJoiner(CF_NAME_SEPARATOR)
                .add(CF_INVERSE_INDEX_PREFIX)
                .add(indexName)
                .add(keyComparatorClass)
                .add(valueComparatorClass)
                .toString();
    }


    /**
     * Parse a column family name which is either an index or inverse index into its constituents.
     * The constituents are:
     * <p>1.INDEX|INVERSE_INDEX</p>
     * <p>2.&lt;index_name&gt;</p>
     * <p>3.&lt;key_comparator_classname&gt;</p>
     * <p>4.&lt;value_comparator_classname&gt;</p>
     * We are embedding the index name, whether the CF is for the
     * forward or inverse index and the comparator class name.
     * TODO this approach for storing 'metadata' in the column family name does not feel optimal
     *  however we need the column families to read any thing from the database
     * @param cfName the name of the column family
     *               -- must be in the format INDEX|INV_INDEX>>>INDEX_NAME>>>&lt;key_comparator-class-name&gt;>>&lt;value_comparator_class_name&gt;
     * @return if the column family is for an index or inverse index -- [INDEX|INV_INDEX, index_name, key_comparator_class_name|NULL, value_comparator_class_name|NULL]
     *      if not -- [null, cfName, null]
     */
    private static String[] parseColumnFamily(String cfName)
    {
        if (isIndexCF(cfName))
        {
            var parts = cfName.split(CF_NAME_SEPARATOR);
            if (parts.length != 4)
            {
                throw new IllegalArgumentException(String.format("%s is not a legal index column family name", cfName));
            }
            if (parts[2].equalsIgnoreCase("NULL")) parts[2] = null;
            if (parts[3].equalsIgnoreCase("NULL")) parts[3] = null;
            return parts;
        }
        else
        {
            return new String[]{
                    null,
                    cfName,
                    null,
                    null
            };
        }
    }

    /**
     * Construct the column family descriptor for the given index
     * @param cfID The column family id which is expected to encode the needed
     *             information
     * @return a column family descriptor, confi
     */
    public static ColumnFamilyDescriptor indexCFD(String cfID)
    {
        var parsedCF = parseColumnFamily(cfID);
        var indexName = parsedCF[1];
        var keyComparatorClassName = parsedCF[2];
        var valueComparatorClassName = parsedCF[3];
        Comparator<byte[]> keyComparator, valueComparator;

        try
        {
            Object kc = keyComparatorClassName == null
                    ? null
                    : Class.forName(keyComparatorClassName).getDeclaredConstructors()[0].newInstance();
            Object vc = valueComparatorClassName ==
                    null ?
                    null :
                    Class.forName(valueComparatorClassName).getDeclaredConstructors()[0].newInstance();
            if (kc instanceof Comparator || kc == null)
            {
                keyComparator = (Comparator<byte[]>)kc;
            }
            else
            {
                throw new HGException(String.format("Comparator class %s is not a comparator", keyComparatorClassName));

            }
            if (vc instanceof Comparator || vc == null)
            {
                valueComparator = (Comparator<byte[]>)vc;
            }
            else
            {
                throw new HGException(String.format("Comparator class %s is not a comparator", valueComparatorClassName));
            }
        }
        catch (Throwable e)
        {
            throw new HGException(String.format("Column family with name %s was present in the RocksDB database," +
                    " but the comparators set for it %s, %s was not found", cfID, keyComparatorClassName, valueComparatorClassName), e);
        }

        //The key and value comparators need to be flipped for the inverse index
        var cfOptions =
                _isIndex(cfID)
                        ? getIndexColumnFamilyOptions(keyComparator, valueComparator, cfID)
                        : getIndexColumnFamilyOptions(valueComparator, keyComparator, cfID);

//        this.cfOptionsStore.put(cfID, cfOptions);
        return new ColumnFamilyDescriptor(cfID.getBytes(StandardCharsets.UTF_8), cfOptions);

    }

    private static ColumnFamilyOptions getIndexColumnFamilyOptions(
            Comparator<byte[]> keyComparator,
            Comparator<byte[]> valueComparator,
            String name)
    {
        var cfOptions = new ColumnFamilyOptions();
        cfOptions.setComparator(new AbstractComparator(new ComparatorOptions())
        {
            @Override
            public String name()
            {
                return name;
            }

            @Override
            public int compare(ByteBuffer buffer1,
                    ByteBuffer buffer2)
            {
                return VarKeyVarValueColumnFamilyMultivaluedDB.compareRocksDBKeys(
                        buffer1,
                        buffer2, keyComparator, valueComparator);
            }
        });
        return cfOptions;
    }



    public void removeIndex(String name)
    {
        synchronized (indexLock)
        {
            var index = indices.remove(name);
            index.close();

            var indexCFHandle = index.getColumnFamilyHandle();

            if (indexCFHandle == null)
            {
                throw new HGException(String.format(
                        "Cannot remove index %s whose column family - %s does not exist.",
                        name, index.getColumnFamilyName()));
            }
            else
            {
                try
                {
                    db.dropColumnFamily(indexCFHandle);
                }
                catch (RocksDBException e)
                {
                    throw new HGException(
                            String.format("Could not delete column family %s which stores " +
                                    "the index %s.", index.getColumnFamilyName(), name), e);
                }
            }
            if (index instanceof BidirectionalRocksDBIndex)
            {
                var biIndex = (BidirectionalRocksDBIndex<?,?>)index;
                var inverseIndexCFHandle = biIndex.getInverseCFHandle();
                if (inverseIndexCFHandle != null)
                {
                    try
                    {
                        db.dropColumnFamily(inverseIndexCFHandle);
                    }
                    catch (RocksDBException e)
                    {
                        throw new HGException(
                                String.format("Could not delete column family %s which stores" +
                                        "the index %s.", biIndex.getInverseCFName(), name), e);
                    }
                }
            }

            indices.remove(name);
        }
    }


    @Override
    public void close()
    {
        synchronized (this.indexLock)
        {
            /*
            Close all indices
             */
            this.indices.values().forEach(RocksDBIndex::close);
            /*
            Close all column families which were not taken responsibility of by an index
             */
            this.preexistingColumnFamilies.close();
        }
    }

    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(
            String name,
            ByteArrayConverter<KeyType> keyConverter,
            ByteArrayConverter<ValueType> valueConverter,
            Comparator<byte[]> keyComparator,
            Comparator<byte[]> valueComparator,
            boolean isBidirectional,
            boolean createIfNecessary)
    {
        RocksDBIndex<KeyType, ValueType> index = (RocksDBIndex<KeyType, ValueType>) indices.get(name);

        if (index != null) return index;
        synchronized (this.indexLock)
        {
            index = (RocksDBIndex<KeyType, ValueType>) indices.get(name);
            if (index != null) return index;

            ColumnFamilyHandle cfHandle, inverseCFHandle = null;
            String cfName = IndexManager.indexCF(name,
                    keyComparator==null
                            ?null
                            :keyComparator.getClass().getName(), valueComparator==null?null:valueComparator.getClass().getName());
            String invCFName = inverseIndexCF(name,
                    keyComparator==null
                            ?null
                            :keyComparator.getClass().getName(), valueComparator==null?null:valueComparator.getClass().getName());

            /*
            we need the column family for the
             */
            if (preexistingColumnFamilies.containsKey(cfName))
            {
                /*
                The preexisting column families together with their options are now responsibility of
                the indices which own them.
                 */
                var preexisting = preexistingColumnFamilies.remove(cfName);
                var invpreexisting = preexistingColumnFamilies.containsKey(invCFName)?preexistingColumnFamilies.remove(invCFName):null;

                cfHandle = preexisting.getFirst();

                if (isBidirectional)
                {
                    inverseCFHandle = invpreexisting==null ? null : invpreexisting.getFirst();
                    if (inverseCFHandle == null)
                    {
                        throw new HGException(String.format("This is probably a bug. The column family %s for the bidirectional index %s" +
                                " exists but there is no column family %s for the inverse index.", name, cfName, invCFName));
                    }
                    index = new BidirectionalRocksDBIndex<KeyType, ValueType>(
                            name,
                            cfHandle,
                            cfName,
                            preexisting.getSecond(),
                            inverseCFHandle,
                            invCFName,
                            invpreexisting.getSecond(),
                            keyConverter,
                            valueConverter,
                            db,
                            store);
                }
                else
                {
                    index = new RocksDBIndex<KeyType, ValueType>(
                            name,
                            cfHandle,
                            cfName,
                            preexisting.getSecond(),
                            keyConverter,
                            valueConverter,
                            db,
                            store);
                }

            }
            else
            {
                /*
                This is a new index
                 */
                var cfo = getIndexColumnFamilyOptions(keyComparator, valueComparator, cfName);
                var cfd = new ColumnFamilyDescriptor(cfName.getBytes(StandardCharsets.UTF_8), cfo);

                try
                {
                    cfHandle = db.createColumnFamily(cfd);
                    index = new RocksDBIndex<KeyType, ValueType>(
                            name,
                            cfHandle,
                            cfName,
                            cfo,
                            keyConverter,
                            valueConverter,
                            db,
                            store);
                }
                catch (RocksDBException e)
                {
                    throw new HGException(e);
                }
                if (isBidirectional)
                {
                    var invCFO = getIndexColumnFamilyOptions(valueComparator, keyComparator, invCFName);
                    var inverseCFD = new ColumnFamilyDescriptor(invCFName.getBytes(StandardCharsets.UTF_8), invCFO);

                    try
                    {
                        inverseCFHandle = db.createColumnFamily(inverseCFD);
                    }
                    catch (RocksDBException e)
                    {
                        throw new HGException(String.format("Error creating column family %s", invCFName), e);
                    }
                    index = new BidirectionalRocksDBIndex<KeyType, ValueType>(
                            name,
                            cfHandle,
                            cfName,
                            cfo,
                            inverseCFHandle,
                            invCFName,
                            invCFO,
                            keyConverter,
                            valueConverter,
                            db,
                            store);
                }
            }
            indices.put(name, index);

            return index;

        }
    }

    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(String name)
    {
        return (HGIndex<KeyType, ValueType>) this.indices.get(name);
    }

    /**
     * Register a preeexisting column family with the index manager so that it is used when the
     * index which corresponds to it is requested
     * @param cfName
     * @param columnFamilyHandle
     * @param options
     */
    public void registerColumnFamily(String cfName, ColumnFamilyHandle columnFamilyHandle, ColumnFamilyOptions options)
    {
        this.preexistingColumnFamilies.registerColumnFamily(cfName, columnFamilyHandle, options);
    }
}
