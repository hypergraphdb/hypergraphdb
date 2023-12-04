/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.util.CountMe;
import org.rocksdb.*;


/**
 * a random access result which is backed by an iterator.
 * @param <T> the type of the values in this result set
 * @implSpec When implementing a concrete class which extends this one,
 *  the following contract should be followed:
 *      extractValue
 *
 */
public abstract class IteratorResultSet<T> implements HGRandomAccessResult<T>, CountMe
{

    protected interface AbstractIterator
    {

        void seek(byte[] keyvalue);

        boolean isValid();

        void seekToFirst();

        void seekToLast();

        void prev();

        void status() throws RocksDBException;

        void next();

        void close();

        byte[] key();
    }
    
    private class RocksDBAbstractIterator implements AbstractIterator, AutoCloseable
    {
        protected final RocksIterator it;

        private RocksDBAbstractIterator(RocksIterator it)
        {
            this.it = it;
        }

        @Override
        public void seek(byte[] keyvalue)
        {
           it.seek(keyvalue);
        }

        @Override
        public boolean isValid()
        {
            return it.isValid();
        }

        @Override
        public void seekToFirst()
        {
            it.seekToFirst();
        }

        @Override
        public void seekToLast()
        {
            it.seekToLast();
        }

        @Override
        public void prev()
        {
            it.prev();
        }

        @Override
        public void status() throws RocksDBException
        {
            it.status();
        }

        @Override
        public void next()
        {
            it.next();
        }

        @Override
        public void close()
        {
            it.close();
        }

        @Override
        public byte[] key()
        {
            return it.key();
        }
    }
    
    private class DistinctRocksDBAbstractIterator extends RocksDBAbstractIterator
    {
        private DistinctRocksDBAbstractIterator(RocksIterator it)
        {
            super(it);
        }

        @Override
        public void prev()
        {
            var current = extractValue();
            while (true)
            {
                it.prev();
                if (it.isValid())
                {
                    var prev = extractValue();
                    if (!prev.equals(current))
                    {
                        break;
                    }
                }
                else
                {
                    break;
                }
            }
        }

        @Override
        public void next()
        {
            var current = extractValue();
            while (true)
            {
                it.next();
                if (it.isValid())
                {
                    var prev = extractValue();
                    if (!prev.equals(current))
                    {
                        break;
                    }
                }
                else
                {
                    break;
                }
            }
        }
    }
    
    protected AbstractIterator iterator;
    private final boolean unique;

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
     *
     * @param iterator
     *         The iterator which backs the result set. All the values in the
     *         iterator are the serializations of the  values in the result
     *         set.
     */
    public IteratorResultSet(RocksIterator iterator)
    {
        this(iterator, false);
    }
    public IteratorResultSet(RocksIterator iterator, boolean unique)
    {
        /*
        TODO is there a better way to filter unique results
         */
        if (unique)
        {
            this.iterator = new DistinctRocksDBAbstractIterator(iterator);
        }
        else
        {
            this.iterator = new RocksDBAbstractIterator(iterator);
        }
        this.unique = unique;

        iterator.seekToFirst();

        if (!iterator.isValid())
        {
            this.checkStatus();
            this.next = this.prev = this.current = OUTSIDE;
            this.lookahead = 0;
        }
        else
        {
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
     * GOTO is
     */
    @Override
    public GotoResult goTo(T value, boolean exactMatch)
    {
//        byte[] valueBytes = toByteConverter.apply(value);
        /*
        TODO
            is GOTO necessary for all subclasses?
            in order to have the goto we need a way to
         */
        byte[] keyvalue = this.toRocksDBKey(value);

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
            current = this.extractValue();
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
                    prev = this.extractValue();
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
                    prev = this.extractValue();
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
                    this.current = this.extractValue();
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
                    current = this.extractValue();
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
            throw new HGException(e);
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
//                var nextval = current;
//                while ()
                iterator.next();
                if (iterator.isValid())
                {
//                    var nextval = this.extractValue();
//                    next = nextval
                    lookahead = 1;
                    next = this.extractValue();
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
                    next = this.extractValue();
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
                    this.current = this.extractValue();
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
                    current = this.extractValue();
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
            return  this.extractValue();
        }
        else
        {
            this.checkStatus();
            return null;
        }
    }

    /**
     * Extract the value at the current position of the result set
     * @return the result set value which is currently pointed to  by the iterator
     * @throws HGException if the iterator is not valid
     */
    protected abstract T extractValue();

    /**
     * convert a result set value to the rocksDB key. Note that this is not
     * simply the serialization of the value, but the rocksdb key which
     * holds the given value.
     * This is only possible if the result set itself allows for the conversion
     * e.g. a result set for all the values of a given key will allow for
     * creation of the rocksdb key by combining the key and value in the
     * correct way
     * @param value
     * @return the rocksdb key which corresponds to the given value
     * @throws HGException
     * @throws UnsupportedOperationException if the specific result set
     * does not allow conversion from a value back to rocksdb key. This
     * may be necessary when the values in the result set do not contain
     * enough information to construct a rocks db key -- e.g. if the result
     * set contains only values
     */
    protected abstract byte[] toRocksDBKey(T value);



     /*
     The result set is backed by an iterator.
       It should be possible to reconstruct the entire key from a value.
      How?
                    The methods returning HGRandomAccessResult<V> are:

                    1. storage.getIncidentResultSet
                       all the values for a single key in the incident column family
                       the incidence column family is fixed key fixed value multi
                       i.e. all the values are stored within the RocksDB key

                       how is the iterator prepared? the iterator limits are
                       the first and last record with that logical key

                       goto()
                       toRocksDBKey() (which is used by goto()) FixedKeyFixedValue.makeRocksDBKey

                       extractValue() FKFV.extractValue

                   2. index.scanKeys -- all the keys in the index cf
                      the index cf is a var key var value multi db
                      a result set containing all the keys.

                      The iterator goes over the entire column family
                      and returns all the logical keys.

                       how is the iterator prepared? the iterator is not limited.
                       It goes over all the records in the column family.

                       goto()
                       toRocksDBKey() (which is used by goto())
                       We have a logical key in the index. we want to
                       position the result set at that key. We need to
                       set the iterator at VKVV.firstRocksDBKey

                       extractValue() -- VKVV.extractKey

                   3. index.scanValues -- all the values in the index cf
                      the index cf is a var key var value multi db

                       how is the iterator prepared? the iterator is not limited.
                       It goes over all the records in the column family.

                       goto()
                       toRocksDBKey() (which is used by goto())
                       we have no way to make this

                       extractValue() -- VKVV.extractValue

                   4. index.find -- all the values for a specific key
                      the index cf is a var key var value multi db

                       how is the iterator prepared? the iterator is limited to the first and last value with the given key.

                       goto()
                       toRocksDBKey() (which is used by goto())
                       VKVV.makeRocksDBKey.

                       extractValue() -- VKVV.extractValue

                      The logical key

                      The ResultSet needs a supplier for:
                      1. extractValue
                      2. toRocksDBKey
                      3. backingIterator

        */
    /**
     * Transform a value to an entire rocksDB key.
     * @param value the value to convert to a rocks db key.
     *
     *
     *
     *
     *
     */
    /*
    Behaviour to customize
    1. extract key/extract value
    2.
     */



}
