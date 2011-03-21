/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.handle;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;

/**
 * <p>
 * An implementation of a live handle that tracks garbage collection activity by
 * extending <code>PhantomReference</code>.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class PhantomHandle extends PhantomReference<Object> implements HGLiveHandle, Comparable<HGHandle>
{
	private HGPersistentHandle persistentHandle;
	private byte flags;
	private static Field refField = null;

	static
	{
		for (Class<? super PhantomReference<Object>> clazz = PhantomReference.class;
			 clazz != null && refField == null;
			 clazz = clazz.getSuperclass())
		{
			Field [] all = clazz.getDeclaredFields();
			for (int i = 0; i < all.length; i++)
				if (all[i].getName().equals("referent"))
				{
					refField = all[i];
					refField.setAccessible(true);
					break;
				}
		}
	}

	/**
	 * <strong>This is for internal use ONLY.</strong>
	 *
	 * <p>See comments in 'getRef' for information about this variable.</p>
	 */
	public static ThreadLocal<Boolean> returnEnqueued = new ThreadLocal<Boolean>();

	public PhantomHandle(Object ref,
						 HGPersistentHandle persistentHandle,
						 byte flags,
						 ReferenceQueue<Object> refQueue)
	{
		super(ref, refQueue);

		this.persistentHandle = persistentHandle;
		this.flags = flags;
	}

	public byte getFlags()
	{
		return flags;
	}

	public HGPersistentHandle getPersistent()
	{
		return persistentHandle;
	}

	/**
	 * <p>
	 * A getter of the referent that uses reflection to access the field directly. Thus,
	 * the field is available even after it's finalized. Therefore, this method should
	 * only be called if it doesn't result in the referent becoming strongly reachable again.
	 * </p>
	 */
	public Object fetchRef()
	{
		try { return refField.get(this); } catch (Exception t) { throw new HGException(t); }
	}

	/**
	 * <p>
	 * A setter of the referent. This setter will block the current Thread while the reference
	 * is being enqueued by the grabage collector.
	 * </p>
	 *
	 * @param ref
	 */
	public void storeRef(Object ref)
	{
		while (isEnqueued())
			try { synchronized (this) { wait(100); } } catch (InterruptedException ex) { }
		try { refField.set(this, ref); } catch (Exception t) { throw new HGException(t); }
	}

	public Object getRef()
	{
		//
		// Here, we want to return null when the object is about to be garbage
		// collected. This will be indicated by the fact that the object will
		// be enqueued in the ReferenceQueue for this PhantomReference.
		//

		// Obviously, we are doing something unorthodox here. The main danger
		// and the first thing to examine in case of weird behavior with this
		// is a situation where an atom gets removed from the 'atoms' WeakHashMap
		// in the cache (hence it's being garbage collected) and 'getRef' is called
		// after that but before the reference gets enqueued. Is that possible at all?
		// It looks like the GC manages references and references queues in a high-priority
		// thread. Also, when an object is going to be garbage collect its references are
		// added to a "pending" list from where they are further enqueued. The 'isEnqueued'
		// method actually checks for "pending or already enqueued" so we should be fine.

		//
		// However, there is a special case in which we don't want to return null: when
		// we are in the phantom reference queue cleanup thread. Because cleanup may involve
		// saving the atom which in turn might trigger a 'getRef' for another handle down
		// in the reference queue, this may result in a deadlock within the phantom ref
		// cleanup thread itself. For this we use the thread local variable 'returnEnqueued'
		//
		Object x = fetchRef();		
		if (isEnqueued())
		{
			Boolean f = returnEnqueued.get();
			if (f != null && f.booleanValue())
				return x;
			x = null;
			do
			{
				try { synchronized (this) { wait(100); } } catch (InterruptedException ex) { }
			} while (isEnqueued());
		}
		return x;
	}

	public void accessed() { }
	
	public final int hashCode()
	{
		return persistentHandle.hashCode();
	}

	public final boolean equals(Object other)
	{
		if (other == null || ! (other instanceof HGHandle))
			return false;
		else if (other instanceof HGLiveHandle)
			return persistentHandle.equals(((HGLiveHandle)other).getPersistent());
		else
			return persistentHandle.equals((HGPersistentHandle)other);
	}

	public String toString()
	{
		return "phantomHandle(" + persistentHandle.toString() + ")";
	}

	public int compareTo(HGHandle h)
	{
		if (h instanceof HGPersistentHandle)
			return this.persistentHandle.compareTo((HGPersistentHandle)h);
		else
			return this.persistentHandle.compareTo(((HGLiveHandle)h).getPersistent());
	}
}
