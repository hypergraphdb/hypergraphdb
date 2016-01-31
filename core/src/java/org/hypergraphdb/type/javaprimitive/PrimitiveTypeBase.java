/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type.javaprimitive;

import java.util.Comparator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGOrderedSearchable;
import org.hypergraphdb.HGException;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.type.HGPrimitiveType;
import org.hypergraphdb.type.HGRefCountedType;
/**
 * <p>
 * A generic, base implementation of the primitive Java types.
 * It maintains an index of all primitive values added for faster lookup. It also shares
 * primitive values for more compact storage - that is, the <code>store</code>
 * method will always return the same handle for an already added primitive value.
 * </p>
 * 
 * <p>
 * Concrete classes for concrete primitive types must provide a name for the 
 * managed index as well as a comparator class for their instance values.
 * </p>
 * 
 * <p>
 * A primitively typed object is translated to a byte []  as 
 * follows:
 * 
 * <ul>
 * <li>The first 4 bytes consitute an unsigned integer - the reference count
 * for the string. Storing a value that's already in the store only 
 * increments this reference count and conversely, removing a value decrements 
 * it. Actual removal occurs when the count reaches 0.</li>
 * <li>The rest of the bytes constitute the actual value of the primitive type.</li>
 * </ul>
 * 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public abstract class PrimitiveTypeBase<JavaType> implements HGPrimitiveType<JavaType>, 
	                                                         HGOrderedSearchable<JavaType, HGPersistentHandle>, 
	                                                         //Comparator<byte[]>,
	                                                         HGRefCountedType
{
    protected HyperGraph graph = null;
    protected HGSortIndex<byte[], HGPersistentHandle> valueIndex = null;
    
    /**
     * <p>Return the <code>Comparator</code> class used for the order relation
     * of the primitive values.</p>
     */
    
    /**
     * <p>Return the name of the DB index to create for the primitive values.</p>
     */
    protected abstract String getIndexName();
    
    /**
     * <p>The offset in the final <code>byte [] </code> of the actual
     * primitive value.</p> 
     */
    protected final static int dataOffset = 4;
    
    protected final HGSortIndex<byte[], HGPersistentHandle> getIndex()
    {
        if (valueIndex == null)
        {
            Comparator<byte[]> comparator = getComparator();
            
            valueIndex = (HGSortIndex<byte[], HGPersistentHandle>)graph.getStore().getIndex(getIndexName(), 
            																			 BAtoBA.getInstance(), 
            																			 BAtoHandle.getInstance(graph.getHandleFactory()),
            																			 comparator,
            																			 null,
            																			 true);

        }
        return valueIndex;
    }
    
    protected final int getRefCount(byte [] buf)
    {
        return BAUtils.readInt(buf, 0);
    }
    
    protected final void putRefCount(int c, byte [] buf)
    {
    	BAUtils.writeInt(c, buf, 0);
    }

    protected final HGPersistentHandle storeImpl(byte [] data)
    {
        HGStore store = graph.getStore();
        
        if (store.hasOverlayGraph())
        {
            return store.store(data);
        }
        
        HGPersistentHandle handle = null;
        
        //
        // First lookup that string value in the DB and return its 
        // handle if available.
        //        
        HGIndex<byte[], HGPersistentHandle> idx = getIndex();
        handle = idx.findFirst(data);
        
        if (handle == null)
        {
            handle = graph.getHandleFactory().makeHandle();
            putRefCount(1, data);            
            store.store(handle, data);
            idx.addEntry(data, handle);
        }
        else
        {
            byte [] ref_counted_data = store.getData(handle);
            putRefCount(getRefCount(ref_counted_data) + 1, ref_counted_data);
            store.store(handle, ref_counted_data);
        }
        
        return handle;
    }
    
    protected abstract byte [] writeBytes(JavaType value);
    protected abstract JavaType readBytes(byte [] data, int offset);
    
    public int compare(byte [] left, byte []right)
    {
        return getComparator().compare(left, right);
    }
     
    public final void setHyperGraph(HyperGraph hg)
    {
        this.graph = hg;
    }

    public final void release(HGPersistentHandle handle)
    {
        HGStore store = graph.getStore();
        byte [] ref_counted_data = store.getData(handle);
        int refCnt = getRefCount(ref_counted_data);
        if (--refCnt > 0)
        {
            putRefCount(refCnt, ref_counted_data);
            store.store(handle, ref_counted_data);
        }
        else
        {
            store.removeData(handle);
            getIndex().removeEntry(ref_counted_data, handle);
        }
    }

    public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
    {                 
        byte [] asBytes = graph.getStore().getData(handle);
        if (asBytes == null)
            throw new HGException("Could not find data for handle: " + handle.toString());
        return readBytes(asBytes, dataOffset);
    } 
    
    private byte [] objectAsBytes(JavaType instance)
    {
        byte data [] = writeBytes(instance);
        byte full [] = new byte[dataOffset + data.length];
        System.arraycopy(data, 0, full, dataOffset, data.length);
        return full;
    }
    
    
    public JavaType fromByteArray(byte[] byteArray, int offset, int length) 
    {
		return readBytes(byteArray, dataOffset + offset);
	}

	public byte[] toByteArray(JavaType object) 
	{
		return objectAsBytes(object);
	}

    @SuppressWarnings("unchecked")
	public HGPersistentHandle store(Object instance)
    {
        return storeImpl(objectAsBytes((JavaType)instance));
    }
    
    public HGSearchResult<HGPersistentHandle> find(JavaType key)
    {
        return getIndex().find(objectAsBytes(key));
    }

    public HGSearchResult<HGPersistentHandle> findGT(JavaType key)
    {
        return getIndex().findGT(objectAsBytes(key));
    }

    public HGSearchResult<HGPersistentHandle> findGTE(JavaType key)
    {
        return getIndex().findGTE(objectAsBytes(key));
    }

    public HGSearchResult<HGPersistentHandle> findLT(JavaType key)
    {
        return getIndex().findLT(objectAsBytes(key));
    }

    public HGSearchResult<HGPersistentHandle> findLTE(JavaType key)
    {
        return getIndex().findLTE(objectAsBytes(key));
    }    
    
    public boolean subsumes(Object l, Object r)
    {
    	if (l == null || r == null)
    		return l == null;
    	else
    		return l.equals(r);        
    }
    
    public int getRefCountFor(JavaType o)
    {
    	byte [] B = objectAsBytes(o);
    	HGPersistentHandle handle = (HGPersistentHandle)this.getIndex().findFirst(B);
    	if (handle == null) return 0;
        byte [] ref_counted_data = graph.getStore().getData(handle);
        int refCnt = getRefCount(ref_counted_data);
    	return refCnt;
    }    
}
