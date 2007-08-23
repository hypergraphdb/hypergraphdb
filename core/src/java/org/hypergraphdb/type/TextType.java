package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.LazyRef;

/**
 * <p>
 * The implementation of the primitive <code>String</code> type for large
 * string values.
 * </p>
 *
 * <p>
 * This implementation records the string using its <code>getBytes</code>
 * method.
 * </p>
 * 
 * <p>
 * Note that by default HyperGraph is configured to use <code>StringType</code>
 * for recording <code>java.lang.String</code> values. Therefore, when adding
 * atoms whose type is <code>TextType</code>, the type must be explicitly
 * specified in the <code>HyperGraph.add</code> method.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class TextType extends HGAtomTypeBase 
{
	public static final HGPersistentHandle HGHANDLE = 
		HGHandleFactory.makeHandle("9e821fcb-de41-11db-8f74-836f1a2faea9");
	
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, LazyRef<HGHandle[]> incidenceSet) 
	{
		if (HGHandleFactory.nullHandle().equals(handle))
			return null;
		byte [] bytes = graph.getStore().getData(handle); 
		return new String(bytes);
	}

	public void release(HGPersistentHandle handle) 
	{
		if (!HGHandleFactory.nullHandle().equals(handle))
			graph.remove(handle);
	}

	public HGPersistentHandle store(Object instance) 
	{
		if (instance == null)
			return HGHandleFactory.nullHandle();
		return graph.getStore().store(instance.toString().getBytes());
	}
}