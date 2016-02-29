package org.hypergraphdb.storage.bje;

import java.util.ArrayList;
import java.util.function.Supplier;

import org.hypergraphdb.TwoWayIterator;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Pair;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * Utility methods
 * 
 * @author borislav
 *
 */
public class bje
{	
	/**
	 * 
	 * <p>
	 * Copy <code>data</code> into the <code>entry</code>. Adjust
	 * <code>entry</code>'s byte buffer if needed.
	 * </p>
	 * 
	 * @param entry
	 * @param data
	 */
	public static void assignData(DatabaseEntry entry, byte[] data)
	{
		byte[] dest = entry.getData();
		if (dest == null || dest.length != data.length)
		{
			dest = new byte[data.length];
			entry.setData(dest);
		}
		System.arraycopy(data, 0, dest, 0, data.length);
	}
	
	/**
	 * Supplies next key/data pair in a cursor - stay one the same key as long as there are duplicates, then go to 
	 * next key etc.
	 */
	public static <T> Supplier<T> nextDataSupplier(final Cursor cursor, final DatabaseEntry stopKey, final ByteArrayConverter<T> converter) 
	{
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		return () -> {
			OperationStatus status = cursor.getNext(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS && (stopKey == null || !HGUtils.eq(key.getData(), stopKey.getData())))
				return converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
			else
				return null;			
		};
	}
	
	/**
	 * Supplies prev key/data pair in a cursor - stay one the key as long as there are duplicates, then go to 
	 * prev key etc.
	 */
	public static <T> Supplier<T> prevDataSupplier(final Cursor cursor, final DatabaseEntry stopKey, final ByteArrayConverter<T> converter) 
	{
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		return () -> {
			if (stopKey != null && HGUtils.eq(key.getData(), stopKey.getData()))
				return null;			
			OperationStatus status = cursor.getPrev(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
			else
				return null;			
		};
	}
	
	/**
	 * Supplies the next value for a given key. Once there are no more duplicates for that key, return null.
	 */
	public static <T> Supplier<T> nextDupSupplier(final Cursor cursor, final DatabaseEntry key, ByteArrayConverter<T> converter) 
	{
		DatabaseEntry data = new DatabaseEntry();
		return () -> {
			OperationStatus status = cursor.getNextDup(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
			else
				return null;			
		};
	}
	
	/**
	 * Supplies the previous duplicate value for a given key. Once there are no more duplicates for that key, return null.
	 */
	public static <T> Supplier<T> prevDupSupplier(final Cursor cursor, final DatabaseEntry key, final ByteArrayConverter<T> converter) 
	{
		DatabaseEntry data = new DatabaseEntry();
		return () -> {
			OperationStatus status = cursor.getPrevDup(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
			else
				return null;			
		};
	}
	
	// this is some continuation passing style in Java: a chain supplier return 
	public static interface ChainSupplier<T> extends Supplier<Pair<T, ChainSupplier<T>>>{};
	public static <T> Supplier<T> toSupplier(ChainSupplier<T> start)
	{
		ArrayList<ChainSupplier<T>> current = new ArrayList<ChainSupplier<T>>();
		current.add(start);
		return () -> {
			while (!current.isEmpty())
			{
				Pair<T, ChainSupplier<T>> p = current.get(0).get();
				if (p.getFirst() != null)
					return p.getFirst();
				current.clear();
				if (p.getSecond() != null)
					current.add(p.getSecond());
			}
			return null;
		};
	}
	
	public static <T> Supplier<T> chain(Supplier<T> start, Supplier<T> next)
	{
		return () -> {
			T result = start.get();
			if (result == null)
				result = next.get();
			return result;
		};
	}
	
	/**
	 * Supplies next key, ignore duplicates, produce the key value, NOT the associated data.
	 */
	public static <T> Supplier<T> nextNoDupSupplier(final Cursor cursor, ByteArrayConverter<T> converter) 
	{
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		return () -> {
			OperationStatus status = cursor.getNextNoDup(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(key.getData(), key.getOffset(), key.getSize());
			else
				return null;
		};
	}	

	/**
	 * Supplies previous key, ignore duplicates, produce the key value, NOT the associated data.
	 */
	public static <T> Supplier<T> prevNoDupSupplier(final Cursor cursor, ByteArrayConverter<T> converter) 
	{
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		return () -> {
			OperationStatus status = cursor.getPrevNoDup(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(key.getData(), key.getOffset(), key.getSize());
			else
				return null;
		};
	}	
	
	public static <T> TwoWayIterator<T> lowerRangeOnKeyResultSet(final Cursor cursor, DatabaseEntry key, ByteArrayConverter<T> converter)
	{
		DatabaseEntry stopKey = new DatabaseEntry(key.getData().clone());
		Supplier<T> nextdup = bje.nextDupSupplier(cursor, stopKey, converter);
		Supplier<T> prevdup = bje.prevDupSupplier(cursor, stopKey, converter);
		Supplier<T> prevdata = bje.prevDataSupplier(cursor, stopKey, converter);
		Supplier<T> nextdata = bje.nextDataSupplier(cursor, stopKey, converter);
		boolean [] onkey = new boolean[] { true };
		return new TwoWayIterator<T>() {

			@Override
			public boolean hasNext() {throw new UnsupportedOperationException(); }				
			public boolean hasPrev() {throw new UnsupportedOperationException(); }				

			@Override
			public T next()
			{
				T result = null;
				if (onkey[0])
				{
					result = nextdup.get();
					if (result == null)
					{
						onkey[0] = false;
						DatabaseEntry value = new DatabaseEntry();
						if (cursor.getPrevNoDup(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
							result = converter.fromByteArray(value.getData(), value.getOffset(), value.getSize()); 
					}
				}
				else
					result = prevdata.get();
				return result;
			}

			@Override
			public T prev()
			{
				T result = null;
				if (onkey[0])
					result = prevdup.get();
				else
				{
					result = nextdata.get();
					if (result == null) // stop key stopped us
					{
						onkey[0] = true;
						result = prevdup.get();
					}						
				}
				return result;
			}			
		};
	}
}