/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSearchable;
import org.hypergraphdb.HGSystemFlags;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.query.impl.UnionResult;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.storage.ByteArrayConverter;

/**
 * <p>
 * Represents the type of a <code>HGAtomRef</code> value. This type implementation
 * handles the behavior of atom references depending on their mode (@see HGAtomRef.java
 * for a thorough description of reference modes and the relationship of an atom reference
 * to its referent).
 * </p>
 * 
 * <p>
 * The implementation maintains a count for all types of references and triggers the correct
 * action (as the case may be) on the referent when all counts go to 0. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class AtomRefType implements HGAtomType, 
								    HGSearchable<HGPersistentHandle, HGPersistentHandle>,
								    ByteArrayConverter<Object>
{
//    public static final HGPersistentHandle HGHANDLE =
//        HGHandleFactory.makeHandle("2ec10476-d964-11db-a08c-eb6f4c8f155a");
//	
	//
	// IMPLEMENTATION NOTE: for a given referent atom, we are reference counting each
	// type of reference (hard, symbolic or floating). Because the 'make' operation should
	// be fast and the 'release' may be slower, we maintain a separate value handle for each
	// type of reference to a given atom. Hence 3 indices that allows us to retrieve the count
	// of a particular reference type from the referent's atom handle.	
	//
	
	private static final String IDX_HARD_DB_NAME = "hg_atomrefs_hard_idx";
	private static final String IDX_SYMBOLIC_DB_NAME = "hg_atomrefs_symbolic_idx";
	private static final String IDX_FLOATING_DB_NAME = "hg_atomrefs_floating_idx";
	
	private static final int MODE_OFFSET = 0; // 1 byte for the mode and 4 for the reference count
	private static final int REFCOUNT_OFFSET = 1; // 1 byte for the mode and 4 for the reference count
	private static final int ATOM_HANDLE_OFFSET = 5; // 1 byte for the mode and 4 for the reference count
	
	private HyperGraph graph;
	private HGIndex<HGPersistentHandle, HGPersistentHandle> hardIdx = null;
	private HGIndex<HGPersistentHandle, HGPersistentHandle> symbolicIdx = null;
	private HGIndex<HGPersistentHandle, HGPersistentHandle> floatingIdx = null;
	
	public HGIndex<HGPersistentHandle, HGPersistentHandle> getHardIdx()
	{
		if (hardIdx == null)
		{
			hardIdx = graph.getStore().getIndex(IDX_HARD_DB_NAME, 
			                                 BAtoHandle.getInstance(graph.getHandleFactory()), 
			                                 BAtoHandle.getInstance(graph.getHandleFactory()), 
			                                 null,
			                                 true);
		}
		return hardIdx;
	}

	public HGIndex<HGPersistentHandle, HGPersistentHandle> getSymbolicIdx()
	{
		if (symbolicIdx == null)
		{
			symbolicIdx = graph.getStore().getIndex(IDX_SYMBOLIC_DB_NAME, 
			                                     BAtoHandle.getInstance(graph.getHandleFactory()), 
			                                     BAtoHandle.getInstance(graph.getHandleFactory()), 
			                                     null,
			                                     true);
		}
		return symbolicIdx;
	}

	public HGIndex<HGPersistentHandle, HGPersistentHandle> getFloatingIdx()
	{
		if (floatingIdx == null)
		{
			floatingIdx = graph.getStore().getIndex(IDX_FLOATING_DB_NAME, 
			                                     BAtoHandle.getInstance(graph.getHandleFactory()), 
			                                     BAtoHandle.getInstance(graph.getHandleFactory()), 
			                                     null,
			                                     true);
		}
		return floatingIdx;
	}
	
//	private class RemovalListener implements HGListener
//	{
//		public HGListener.Result handle(HyperGraph hg, HGEvent event)
//		{
//			HGAtomRemoveRequestEvent ev = (HGAtomRemoveRequestEvent)event;
//			HGPersistentHandle pHandle = hg.getPersistentHandle(ev.getAtomHandle());
//			if (getHardIdx().findFirst(pHandle) != null ||
//				getFloatingIdx().findFirst(pHandle) != null) // symbolic links don't prevent removal of atoms
//				return Result.cancel;
//			else
//				return Result.ok;
//		}
//	}
//
//	private RemovalListener removalListener = new RemovalListener();
	
	public void setHyperGraph(HyperGraph hg) 
	{
	    // unlikely that we would ever change the HyperGraph instance, but who knows....
//		if (this.graph != null) 
//			this.graph.getEventManager().removeListener(HGAtomRemoveRequestEvent.class, removalListener);
		this.graph = hg;
//		hg.getEventManager().addListener(HGAtomRemoveRequestEvent.class, removalListener);
	}

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		byte [] data = graph.getStore().getData(handle);
		HGAtomRef.Mode mode = HGAtomRef.Mode.get(data[MODE_OFFSET]);
		HGPersistentHandle atomHandle = graph.getHandleFactory().makeHandle(data, ATOM_HANDLE_OFFSET);
		return new HGAtomRef(atomHandle, mode);
	}

	public HGPersistentHandle store(Object instance) 
	{
		HGAtomRef ref = (HGAtomRef)instance;
		HGPersistentHandle refHandle = graph.getPersistentHandle(ref.getReferent());
		HGPersistentHandle valueHandle;
		HGIndex<HGPersistentHandle, HGPersistentHandle> idx;
		switch (ref.getMode())
		{
			case hard:
				idx = getHardIdx();
				break;
			case symbolic:			
				idx = getSymbolicIdx();
				break;
			case floating:
				idx = getFloatingIdx();
				break;
			default:
				idx = null; // impossible				
		}
		valueHandle = idx.findFirst(refHandle);
		int handleSize = refHandle.toByteArray().length;
		if (valueHandle == null)
		{
			// we store the mode followed by the handle of the referent followed by a reference count
			byte [] data = new byte[5 + handleSize];
			data[MODE_OFFSET] = ref.getMode().getCode();
			System.arraycopy(refHandle.toByteArray(), 0, data, ATOM_HANDLE_OFFSET, handleSize);
			BAUtils.writeInt(1, data, REFCOUNT_OFFSET);
			valueHandle = graph.getStore().store(data);
			idx.addEntry(refHandle, valueHandle);
		}
		else
		{
			byte [] data = graph.getStore().getData(valueHandle);
			BAUtils.writeInt(BAUtils.readInt(data, REFCOUNT_OFFSET) + 1, data, REFCOUNT_OFFSET);
			graph.getStore().store(valueHandle, data);
		}
		return valueHandle;
	}
	
	public void release(HGPersistentHandle handle) 
	{
		byte [] data = graph.getStore().getData(handle);
		HGAtomRef.Mode mode = HGAtomRef.Mode.get(data[MODE_OFFSET]);
		int count = BAUtils.readInt(data, REFCOUNT_OFFSET) - 1;
		if (count == 0)
		{
			boolean makeManaged = false;
			boolean removeRef = false;
			HGPersistentHandle otherRef = null;	
			HGPersistentHandle refHandle = graph.getHandleFactory().makeHandle(data, ATOM_HANDLE_OFFSET);
			switch (mode)
			{
				case hard:
				{
					otherRef = getFloatingIdx().findFirst(refHandle);					
					if (otherRef != null)
					{
						makeManaged = true;
						removeRef = BAUtils.readInt(graph.getStore().getData(otherRef), REFCOUNT_OFFSET) == 0;
					}
					else
						removeRef = true;
					getHardIdx().removeAllEntries(refHandle);
					break;
				}
				case symbolic:
				{
					graph.getStore().removeData(handle);
					getSymbolicIdx().removeAllEntries(refHandle);
					break;
				}	
				case floating:
				{
					makeManaged = true;
					otherRef = getHardIdx().findFirst(refHandle);
					removeRef = otherRef == null || BAUtils.readInt(graph.getStore().getData(otherRef), REFCOUNT_OFFSET) == 0;
					getFloatingIdx().removeAllEntries(refHandle);
					break;
				}
			}
			if (removeRef)
			{
				graph.getStore().removeData(handle);
				if (otherRef != null)
					graph.getStore().removeData(otherRef);
				if (makeManaged)
				{
					int flags = graph.getSystemFlags(refHandle);
					if ((flags & HGSystemFlags.MANAGED) == 0)
						graph.setSystemFlags(refHandle, flags | HGSystemFlags.MANAGED);
				}
				else
					graph.remove(refHandle);
			}			
		}
		else
		{
			BAUtils.writeInt(count, data, REFCOUNT_OFFSET);
			graph.getStore().store(handle, data);
		}
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return ((HGAtomRef)general).getReferent().equals(((HGAtomRef)specific).getReferent());
	}

	/**
	 * The key is expected to be of type <code>HGAtomRef</code> OR of 
	 * type <code>HGHandle</code>. In the former case, references with the specific
	 * mode and referent are search. In the latter or if the mode of the HGAtomRef is null, 
	 * all reference regardless of mode are searched.
	 * 
	 */
	@SuppressWarnings("unchecked")	
	public HGSearchResult<HGPersistentHandle> find(HGPersistentHandle key) 
	{
		if (key instanceof HGAtomRef)
		{
			HGAtomRef ref = (HGAtomRef)key;
			HGPersistentHandle pHandle = graph.getPersistentHandle(ref.getReferent());
			switch (ref.getMode())
			{
				case hard:
					return getHardIdx().find(pHandle);
				case symbolic:
					return getSymbolicIdx().find(pHandle);
				case floating:
					return getFloatingIdx().find(pHandle);
			}
		}
		HGPersistentHandle referent = graph.getPersistentHandle((HGHandle)key);
		return new UnionResult(getHardIdx().find(referent), 
							   new UnionResult(getSymbolicIdx().find(referent),
									   		   getFloatingIdx().find(referent)));
	}

	public Object fromByteArray(byte[] byteArray, int offset, int length)
	{
		HGPersistentHandle h = BAtoHandle.getInstance(graph.getHandleFactory()).fromByteArray(byteArray, offset, length);
		if (h.equals(graph.getHandleFactory().nullHandle()))
			return null;
		else
			return graph.get(h);
	}

	public byte[] toByteArray(Object object)
	{
		if (object == null)
			return graph.getHandleFactory().nullHandle().toByteArray();
		else
			return graph.getPersistentHandle(graph.getHandle(object)).toByteArray();
	}
}
