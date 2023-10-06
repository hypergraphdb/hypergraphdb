/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.util.CountMe;
import org.rocksdb.*;

import java.util.function.Function;
import static org.hypergraphdb.storage.rocksdb.FixedKeyFixedValueColumnFamilyMultivaluedDB.*;

/**
 *
 * A result set which represents the values for a single key in a
 * multivalued database
 * The database is assumed to be multivalued. i.e. the values will be stored
 * as part of the RocksDB keys
 * If the database is not
 * @param <T> the type of the elements in this result set. T must have a meaningful
 *           implementation of {@link Object#equals(Object)}
 *
 * TODO cleanup the concepts and the API
 *  what is an IndexResultSet?
 *  We shouldn't be having the need for a key;
 *  The concept of a key belongs to the single key result set
 *  an index result set is a range of values in an ordered index
 *  Single key result set is a range of values for a single key (a single rocksdb key
 *  as opposed to the logical hgdb level key ).
 *
 */
public class SingleKeyResultSet<T> implements HGRandomAccessResult<T>,
        CountMe
{
    private final ReadOptions iteratorReadOptions;
    public RocksIterator iterator;
    byte[] logicalKey;
    Function<T, byte[]> toByteConverter;
    private final Function<byte[], T> fromByteConverter;

    /*
    TODO come up with a good way to make this typesafe i.e. this needs to be of type T
    Denotes an unknown value
     */
    private final Object UNKNOWN = new Object();

    /*
     denotes the end of the cursor at each end
     */
    /*
    We need a logical value which denotes a value outside of the result set
     */
    private final T OUTSIDE = null;

    /*
    The logical current value.
    it is different from the underlying's iterator.current() by lookahead
    positions
     i.e.position(iterator.current) + offset = position(current)
     */
    private Object current;
    /*
    when we need to examine next or prev
    conceptually, in order to move the cursor
     */
    private Object next;
    private Object prev;

    /**
    the lookahead is either -1, 0, or 1
    it serves to designate the position of the cursor in relation to
    current.

    0 means that the cursor points to the same element as the current
    1 means the cursor is one element ahead of current
    -1 means the cursor is one element behind current

    we need that in order to position before the start of the
    result set or after the end of the result set or to inspect the adjacent
    elements without changing our position
     */
    private int lookahead = 0;

    /*
    invariant: when we move the iterator, the position which corresponds to the
    iterator is populated i.e
    if lookahead == 0, current is not null unless the iterator has no elements
    if lookahead == 1, next is not null unless current is the last element of the iterator
    if lookahead == -1, prev is not null unless current is the first element of the iterator

    empty iterator:
    lookahead == 0
    current == prev == next == OUTSIDE

    before first:
    current == prev == OUTSIDE
    next = it.key()

    after last:
    current == next == OUTSIDE
    prev = it.key()
     */


    /*
    TODO
        the public constructor should only take db, column family,
        and key.
        Then we should make sure the iterator is valid.
        If the iterator is not valid, there are no records for that key
        so the move operations should not do anything
     */

    /**
     * The result set is associated with a transaction.
     * TODO what happens with the iterator when the transaction is committed/
     *  rolled back?
     * @param txn
     * @param columnFamilyHandle
     * @param logicalKey
     * @param toByteConverter
     * @param fromByteConverter
     */
    public SingleKeyResultSet(
            StorageTransactionRocksDB txn,
            ColumnFamilyHandle columnFamilyHandle,
            byte[] logicalKey,
            Function<T, byte[]> toByteConverter,
            Function<byte[], T> fromByteConverter)
    {

        var first = new Slice(
                FixedKeyFixedValueColumnFamilyMultivaluedDB.firstRocksDBKey(logicalKey));
        var last = new Slice(
                FixedKeyFixedValueColumnFamilyMultivaluedDB.lastRocksDBKey(logicalKey));

        this.iteratorReadOptions = new ReadOptions().setIterateLowerBound(first).setIterateUpperBound(last);

        this.iterator = txn.rocksdbTxn().getIterator(
                this.iteratorReadOptions,
                columnFamilyHandle);

        iterator.seekToFirst();
        this.logicalKey = logicalKey;
        this.toByteConverter = toByteConverter;
        this.fromByteConverter = fromByteConverter;

        if (!iterator.isValid())
        {
            this.checkStatus();
            /*
            No values with that key, the result set is empty.
             */
            this.next = this.prev = this.current = OUTSIDE;
            this.lookahead = 0;
        }
        else
        {
//            this.prev = this.next = UNKNOWN;
//            this.current = fromByteConverter.apply(extractValue(iterator.key()));
            goBeforeFirst();
        }
    }

    private boolean isEmpty()
    {
        return this.current == OUTSIDE && this.prev == OUTSIDE && this.next == OUTSIDE;
    }




    /**
     *
     * Position the result set at a specific value
     *<br/>
     *
     *
     * ______________________________________
     * Implementation:
     *  If the result set is empty, it remains empty
     *  If a GTE value is found, the iterator is placed at it and the LA = 0
     *  If a GTE value is not found, the iterator is placed at the beginning
     *  of the result set and LA = 0
     *
     *  exactMatch does not actually control how the iterator is placed
     *  it just controls whether GotoResult.nothing or GotoResult.close
     *  is returned when a GT value is found and not an exact match
     *  _____________________________________
     *  API:
     *
     * If the value is present in the result set, the result set is moved
     * to that value. If a greater value is present in the result set and the
     * exactMatch argument is false, the result set is moved to the first
     * greater value.
     *
     *
     * If the result set is empty, no transformation to the result set are
     *                   made and no exception is raised
     *
     * If the result set is not empty but the provided value is not present (or no
     *                   value greater than the provided is present in case
     *                   exactMatch is false), the position within the result
     *                   set is not defined.
     *
     * @param value The value where this result set should be positioned.
     * @param exactMatch A flag indicating whether the passed in value should
     * match exactly a value in the result set, or whether the cursor should
     * be positioned to the closest value. Here "closest" means "smallest
     * greater than the <code>value</code> parameter.
     * <br/>
     * @return GotoResult.nothing if the result set is empty or it is not
     *  empty, but it does not contain the provided value ()
     *
     */
    @Override
    public GotoResult goTo(T value, boolean exactMatch)
    {
        byte[] valueBytes = toByteConverter.apply(value);
        byte[] keyvalue = makeRocksDBKey(this.logicalKey, valueBytes);

        /*
         ensure the result set is not empty.
         if it is, there is no need to do anything.
         */

        if (this.isEmpty())
        {
            /*
            if the result set is empty, don't do anything
             */
            return GotoResult.nothing;
        }

        this.iterator.seek(keyvalue);
        /*
        After this the iterator is at the first element
         */

        if (this.iterator.isValid())
        {
            this.current = this.iteratorValue();
            this.lookahead = 0;
            this.next = this.prev = UNKNOWN;

            if (value.equals(current))
                return GotoResult.found;
            else if (exactMatch)
                return GotoResult.nothing;
            else
                return GotoResult.close;

        }
        else
        {
            this.checkStatus();
            /*
            The iterator is invalid, no values after this one
            reset it to the first value for that key which is guaranteed
            to exist because we checked whether the iterator is empty.
             */
            iterator.seekToFirst();
            current = fromByteConverter.apply(extractValue(keyvalue));
            lookahead = 0;
            prev = OUTSIDE;
            next = UNKNOWN;
            return GotoResult.nothing;
        }
    }

    /**
     * make the result set 'empty'
     */
    private void empty()
    {
        current = next = prev = OUTSIDE;
        lookahead = 0;
    }

    @Override
    public void goAfterLast()
    {
        this.iterator.seekToLast();
        if (this.iterator.isValid())
        {
            this.lookahead = -1;
            this.prev = this.iteratorValue();
            this.current = this.next = OUTSIDE;
        }
        else
        {
            this.checkStatus();
            /*
            if seekToLast resulted in invalid state, then there are no
            values in the iterator
             */
            this.empty();
        }
    }


    @Override
    public void goBeforeFirst()
    {
        this.iterator.seekToFirst();

        if (this.iterator.isValid())
        {
            this.lookahead = 1;
            this.next = this.iteratorValue();
            this.current = this.prev = OUTSIDE;
        }
        else
        {
            this.checkStatus();
            this.empty();
        }
    }

    /**
     *
     * @return
     */
    @Override
    public T current()
    {
        /*
        current is always populated
         */
        assert this.current != UNKNOWN;
        return (T) this.current;
    }


    @Override
    public boolean hasPrev()
    {
        if (this.isEmpty())
        {
            /*
            if the result set is empty, do not to anything
            this will actually be handled by the next if as well
            TODO consider removing this check
             */
            return false;
        }

        if (prev != UNKNOWN)
        {
            //empty RS is here
            return prev != OUTSIDE;
        }
        else
        {
            if (lookahead == -1)
            {
                /*
                the iterator is positioned before the current record
                this is not allowed because if LA =  -1 prev is guaranteed
                to be known
                 */
                throw new IllegalStateException("prev should have been populated");
            }
            else if (lookahead == 0)
            {
                iterator.prev();
                if (iterator.isValid())
                {
                    lookahead = -1;
                    prev = fromByteConverter.apply(extractValue(iterator.key()));
                    return true;
                }
                else
                {
                    this.checkStatus();
                    /*
                    todo if the iterator is already invalid, will this work??
                    if we dropped from the end, go to the last element,
                    which was the current value
                     */
                    iterator.seekToFirst();
                    prev = OUTSIDE;
                    return false;
                }
            }
            else if (this.lookahead == 1)
            {
                iterator.prev();
                /*
                we moved the iterator to the current record, so it must be
                valid
                 */
                if (!iterator.isValid())
                {
                    this.checkStatus();
                    throw new RuntimeException("Iterator is expected to be valid");
                }

                /*
                Move the iterator to the position after the next
                 */
                iterator.prev();
                if (iterator.isValid())
                {
                    lookahead = -1;
                    prev = fromByteConverter.apply(extractValue(iterator.key()));
                    return true;
                }
                else
                {
                    this.checkStatus();
                    /*
                    current is the last position in the result set
                     */
                    iterator.seekToFirst();
                    //the iterator points to the current position
                    lookahead = 0;
                    /*
                    we moved to the current position, so it must be valid
                     */
                    if(!iterator.isValid())
                    {
                        this.checkStatus();
                        throw new RuntimeException("Iterator was expected to be valid");
                    }
                    prev = OUTSIDE;
                    return false;
                }

            }
            throw new RuntimeException("shouldnt be here");
        }

    }

    @Override
    public T prev()
    {
        if (this.isEmpty())
            return null;

        if (this.prev != UNKNOWN)
        {
            this.next = this.current;
            this.current = this.prev;
            this.prev = UNKNOWN;
            /*
             * why decreasing lookahead is ok?
             */
            assert this.lookahead <= 0;
            this.lookahead++;
            return (T)this.current;
        }
        else
        {
            if (this.lookahead == -1)
            {
                //the cursor is at prev, so it must have been populated
                throw new IllegalStateException("prev should have been populated");
            }
            else if (this.lookahead == 0)
            {
                this.iterator.prev();
                if (this.iterator.isValid())
                {
                    this.next = this.current;
                    this.current = fromByteConverter.apply(extractValue(this.iterator.key()));
                    //lookahead is still 0;
                    this.prev = UNKNOWN;
                }
                else
                {
                    this.checkStatus();
                    this.goBeforeFirst();
                    return (T) null;
                }

            }
            else if (this.lookahead == 1)
            {
                /*
                The cursor is ahead of the current value
                 */
                this.iterator.prev();
                this.iterator.prev();

                if (this.iterator.isValid())
                {
                    this.next = this.current;
                    current = fromByteConverter.apply(extractValue(this.iterator.key()));
                    this.lookahead = 0;
                    this.prev= UNKNOWN;
                }
                else
                {
                    this.checkStatus();
                    this.goBeforeFirst();
                    return (T) null;
                }
            }
        }
        throw new RuntimeException("shouldnt be here");
    }

    private void checkStatus()
    {
        try
        {
            this.iterator.status();
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        }
    }
    @Override
    public boolean hasNext()
    {
        if (this.isEmpty())
        {
            /*
            if the result set is empty, do not to anything
            this will actually be handled by the next if as well
            TODO consider removing this check
             */
            return false;
        }

        if (next != UNKNOWN)
        {
            //empty RS is here
            return next != OUTSIDE;
        }
        else
        {
            if (lookahead == 1)
            {
                /*
                the iterator is positioned after the current record
                this is not allowed because if LA =  1 next is guaranteed
                to be known
                 */
                throw new IllegalStateException("next should have been populated");
            }
            else if (lookahead == 0)
            {
                iterator.next();
                if (iterator.isValid())
                {
                    lookahead = 1;
                    next = fromByteConverter.apply(extractValue(iterator.key()));
                    return true;
                }
                else
                {
                    this.checkStatus();
                    /*
                    todo if the iterator is already invalid, will this work??
                    if we dropped from the end, go to the last element,
                    which was the current value
                     */
                    iterator.seekToLast();
                    next = OUTSIDE;
                    return false;
                }
            }
            else if (this.lookahead == -1)
            {
                iterator.next();
                /*
                we moved the iterator to the current record, so it must be
                valid
                 */
                if (!iterator.isValid())
                {
                    this.checkStatus();
                    throw new RuntimeException("Iterator is expected to be valid");
                }


                /*
                Move the iterator to the position after the next
                 */
                iterator.next();
                if (iterator.isValid())
                {
                    lookahead = 1;
                    next = fromByteConverter.apply(extractValue(iterator.key()));
                    return true;
                }
                else
                {
                    this.checkStatus();
                    /*
                    current is the last position in the result set
                     */
                    iterator.seekToLast();
                    //the iterator points to the current position
                    lookahead = 0;
                    /*
                    we moved to the current position, so it must be valid
                     */
                    if (!iterator.isValid())
                    {
                        this.checkStatus();
                        throw new RuntimeException("Iterator is expected to be valid");
                    }
                    next = OUTSIDE;
                    return false;
                }

            }
            throw new RuntimeException("shouldnt be here");
        }

    }

    @Override
    public T next()
    {
        if (this.isEmpty())
            return null;

        if (this.next != UNKNOWN)
        {
            this.prev = this.current;
            this.current = this.next;
            this.next = UNKNOWN;
            /*
             * why decreasing lookahead is ok?
             */
            assert this.lookahead >= 0;
            this.lookahead--;
            return (T)this.current;

        }
        else
        {
            if (this.lookahead == 1)
            {
                //the cursor is at next, so it must have been populated
                throw new IllegalStateException("prev should have been populated");
            }
            else if (this.lookahead == 0)
            {
                this.iterator.next();
                if (this.iterator.isValid())
                {
                    this.prev = this.current;
                    this.current = fromByteConverter.apply(extractValue(this.iterator.key()));
                    //lookahead is still 0;
                    next = UNKNOWN;
                }
                else
                {
                    this.checkStatus();
                    this.goAfterLast();
                    return (T) null;
                }

            }
            else if (this.lookahead == -1)
            {
                /*
                The cursor is behind the current value
                this can happen when we are at a specific value,
                we first called hasPrev, and then hasNext
                 */
                this.iterator.next();
                this.iterator.next();

                if (this.iterator.isValid())
                {
                    prev = current;
                    current = fromByteConverter.apply(extractValue(this.iterator.key()));
                    this.lookahead = 0;
                    next = UNKNOWN;
                }
                else
                {
                    this.checkStatus();
                    this.goAfterLast();
                    return (T) null;
                }
            }
        }
        throw new RuntimeException("shouldnt be here");
    }

    @Override
    public void close()
    {
        this.iteratorReadOptions.close();
        this.iterator.close();
    }

    @Override
    public boolean isOrdered()
    {
        return true;
    }


    /**
     *
     * This counts all the elements in the iterator sequentially!
     *
     * Implementation:
     *  After calling this the result set is set to the first element.
     *  If the result set is empty, the result set state is not changed
     *
     * @return
     */
    @Override
    public int count()
    {
        if (this.isEmpty())
        {
            return 0;
        }

        int i = 0;
        iterator.seekToFirst();
        while (iterator.isValid())
        {
            i++;
            iterator.next();
        }

        //the iterator is valid after this because it is not empty
        iterator.seekToFirst();
        if (!iterator.isValid())
        {
            try
            {
                iterator.status();
            }
            catch (RocksDBException e)
            {
                throw new RuntimeException(e);
            }
        }
        prev = next = UNKNOWN;
        current = iteratorValue();
        return i;
    }

    /**
     * The value of the record currently pointed by the iterator
     * @return the current value the iterator is pointing to, null if
     * the iterator is not valid
     */
    private T iteratorValue()
    {
        if (iterator.isValid())
        {
            return  fromByteConverter.apply(extractValue(iterator.key()));
        }
        else
        {
            this.checkStatus();
            return null;
        }
    }


}
