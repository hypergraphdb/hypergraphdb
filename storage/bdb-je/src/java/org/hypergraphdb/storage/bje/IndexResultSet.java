/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted software. For permitted
 * uses, licensing options and redistribution, please see the LicensingInformation file at the root level of
 * the distribution.
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc. All rights reserved.
 */
package org.hypergraphdb.storage.bje;

import java.util.NoSuchElementException;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.CountMe;
import org.hypergraphdb.util.HGUtils;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * <p>
 * An <code>IndexResultSet</code> is based on a cursor over an indexed set of values. Implementation of
 * complex query execution may move the cursor position based on some index key to speed up query processing.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public abstract class IndexResultSet<T> implements HGRandomAccessResult<T>, CountMe {
	protected static final Object UNKNOWN = new Object();

	/**
	 * The underlying cursor
	 */
	protected BJETxCursor cursor;

	/**
	what are current, prev and next?
	 */
	protected Object current = UNKNOWN, prev = UNKNOWN, next = UNKNOWN;


	/**
	Those key and data are passed to all cursor operations, either
	as input, or output
	 */
	protected DatabaseEntry key;
	/**
	 Those key and data are passed to all cursor operations, either
	 as input, or output
	 */
	protected DatabaseEntry data = new DatabaseEntry();

	/**
	From database serialization to type
	 */
	protected ByteArrayConverter<T> converter;

	// what is this?
	protected int lookahead = 0;

	/**
	 * Silently close the cursor
	 */
	protected final void closeNoException() {
		try {
			close();
		}
		catch (Throwable t) {
		}
	}

	/**
	 * Check whether the cursor is open
	 */
	protected final void checkCursor() {
		if (!cursor.isOpen())
			throw new HGException(
					"DefaultIndexImpl.IndexResultSet: attempt to perform an operation on a closed or invalid cursor.");
	}

	/**
	 * 
	 * <p>
	 * Copy <code>data</code> into the <code>entry</code>. Adjust <code>entry</code>'s byte buffer if needed.
	 * </p>
	 * 
	 * @param entry the destination
	 * @param data the source
	 */
	protected void assignData(DatabaseEntry entry, byte[] data) {
		byte[] dest = entry.getData();
		if (dest == null || dest.length != data.length) {
			dest = new byte[data.length];
			entry.setData(dest);
		}
		System.arraycopy(data, 0, dest, 0, data.length);
	}

	/**
	 * just shuffle prev, current, next and lookahead
	 */
	protected final void moveNext() {
		//        checkCursor();
		prev = current;
		current = next;
		next = UNKNOWN;
		lookahead--;
		/*
		 * while (true) { next = advance(); if (next == null) break; if (++lookahead == 1) break; }
		 */
	}

	/**
	 * just shuffle prev, current, next and lookahead
	 */
	protected final void movePrev() {
		//        checkCursor();
		next = current;
		current = prev;
		prev = UNKNOWN;
		lookahead++;
		/*
		 * while (true) { prev = back(); if (prev == null) break; if (--lookahead == -1) break; }
		 */
	}

	/**
	 *
	 * @return
	 */
	protected abstract T advance();

	/**
	 *
	 * @return
	 */
	protected abstract T back();

	/**
	 * <p>
	 * Construct an empty result set.
	 * </p>
	 */
	protected IndexResultSet() {
	}

	//    static int idcounter = 0;    
	//    int id = 0;

	/**
	 * <p>
	 * Construct a result set matching a specific key.
	 * </p>
	 * 
	 * @param cursor
	 * @param key
	 */
	public IndexResultSet(BJETxCursor cursor, DatabaseEntry keyIn, ByteArrayConverter<T> converter) {
		/*
		 * id = idcounter++; System.out.println("Constructing index set with id " + id); StackTraceElement
		 * e[]=Thread.currentThread().getStackTrace(); for (int i=0; i <e.length; i++) { System.out.println(e[i]);
		 * }
		 */
		this.converter = converter;
		this.cursor = cursor;
		this.key = new DatabaseEntry();

		/*
		First, if the user has supplied a keyIn,
		 */
		if (keyIn != null) {
			assignData(this.key, keyIn.getData());
		}
		
		try {
			//what if the cursor is not set?
			/*
			key, data are outputs, so we are overriding them.
			 */
			cursor.cursor().getCurrent(key, data, LockMode.DEFAULT);
			next = converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
			lookahead = 1;
		}
		catch (Throwable t) {
			throw new HGException(t);
		}
	}

	protected void positionToCurrent(byte[] data, int offset, int length) {
		current = converter.fromByteArray(data, offset, length);
		lookahead = 0;
		prev = next = UNKNOWN;
	}

	public void goBeforeFirst() {
		try {
			if (cursor.cursor().getFirst(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				current = UNKNOWN;
				prev = null;
				next = converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
				lookahead = 1;
			}
			else {
				prev = next = null;
				current = UNKNOWN;
				lookahead = 0;
			}
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	public void goAfterLast() {
		try {
			if (cursor.cursor().getLast(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				current = UNKNOWN;
				next = null;
				prev = converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
				lookahead = -1;
			}
			else {
				prev = next = null;
				current = UNKNOWN;
				lookahead = 0;
			}
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	public GotoResult goTo(T value, boolean exactMatch) {
		byte[] B = converter.toByteArray(value);
		assignData(data, B);
		try {
			OperationStatus status = null;
			if (exactMatch) {
				status = cursor.cursor().getSearchBoth(key, data, LockMode.DEFAULT);
				if (status == OperationStatus.SUCCESS) {
					positionToCurrent(data.getData(), data.getOffset(), data.getSize());
					return GotoResult.found;
				}
				else
					return GotoResult.nothing;
			}
			else {
				status = cursor.cursor().getSearchBothRange(key, data, LockMode.DEFAULT);
				if (status == OperationStatus.SUCCESS) {
					GotoResult result = HGUtils.eq(B, data.getData()) ? GotoResult.found : GotoResult.close;
					positionToCurrent(data.getData(), data.getOffset(), data.getSize());
					return result;
				}
				else
					return GotoResult.nothing;
			}
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	/**
	 * Close the underlying cursor
	 */
	public final void close() {
		if (cursor == null)
			return;

		try {
			current = next = prev = UNKNOWN;
			key = null;
			cursor.close();
		}
		catch (Throwable t) {
			throw new HGException("Exception while closing a DefaultIndexImpl cursor: " + t.toString(), t);
		}
		finally {
			cursor = null;
		}
	}

	public final T current() {
		if (current == UNKNOWN)
			throw new NoSuchElementException();
		return (T)current;
	}

	public final boolean hasPrev() {
		if (prev == UNKNOWN) {
			while (lookahead > -1) {
				prev = back();
				if (prev == null)
					break;
				lookahead--;
			}
			//    		prev = back();
		}
		return prev != null;
	}

	public final boolean hasNext() {
		if (next == UNKNOWN) {
			while (lookahead < 1) {
				next = advance();
				if (next == null)
					break;
				lookahead++;
			}
			//    		next = advance();
		}
		return next != null;
	}

	public final T prev() {
		if (!hasPrev())
			throw new NoSuchElementException();
		movePrev();
		return current();
	}

	public final T next() {
		if (!hasNext())
			throw new NoSuchElementException();
		moveNext();
		return current();
	}

	public final void remove() {
		throw new UnsupportedOperationException("HG - IndexResultSet does not implement remove.");
	}

	public int count() {
		try {
			return cursor.cursor().count();
		}
		catch (DatabaseException ex) {
			throw new HGException(ex);
		}
	}

	/**
	 * Remove current element. After that cursor becomes invalid, so next(), prev() operations will fail.
	 * However, a goTo operation should work.
	 */
	public void removeCurrent() {
		try {
			cursor.cursor().delete();
		}
		catch (DatabaseException ex) {
			throw new HGException(ex);
		}
	}	
}
