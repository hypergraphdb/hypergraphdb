/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.HGDepthFirstTraversal;
import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.TypePlusCondition;
import org.hypergraphdb.transaction.HGTransactionException;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * A set of static utility methods operating on types.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public final class TypeUtils 
{
	/**
	 * <p>Split a dimension path in dot notation to a <code>String[]</code>.</p>
	 *  
	 * @param formatted The dimension path in the format "a.b.c...d" 
	 * @return The <code>String[]</code>  {"a", "b", "c", ..., "d"} 
	 */
	public static String [] parseDimensionPath(String formatted)
	{
		StringTokenizer tok = new StringTokenizer(formatted, ".");
		String [] result = new String[tok.countTokens()];
		for (int i = 0; i < result.length; i++)
			result[i] = tok.nextToken();
		return result;
	}
	
	public static String formatDimensionPath(String [] dimPath)
	{
		StringBuffer result = new StringBuffer();
		for (int i = 0; i < dimPath.length; i++)
		{
			result.append(dimPath[i]);
			if (i < dimPath.length - 1)
				result.append(".");
		}	
		return result.toString();
	}
	
	public static HGProjection getProjection(HyperGraph hg, HGAtomType type, String [] dimPath)
	{
		HGProjection proj = null;
		for (int j = 0; j < dimPath.length; j++)
		{
			if (! (type instanceof HGCompositeType))
				return null;
			proj = ((HGCompositeType)type).getProjection(dimPath[j]);
			if (proj == null)
				return null;
			type = (HGAtomType)hg.get(proj.getType());
		}
		return proj;
	}
	
	public static HGTypedValue project(HyperGraph hg,
									   HGHandle type, 
									   Object value, 
									   String [] dimPath,
									   boolean throwExceptionOnNotFound)
	{
		
		for (int j = 0; j < dimPath.length; j++)
		{
			HGAtomType tinstance = (HGAtomType)hg.get(type);
			if (! (tinstance instanceof HGCompositeType))
				if (throwExceptionOnNotFound)
					throw new HGException("Only composite types can be indexed by value parts: " + value
						+ ", during projection of " + formatDimensionPath(dimPath) + 
						" in type " + tinstance);
				else
					return null;
			HGProjection proj = ((HGCompositeType)tinstance).getProjection(dimPath[j]);
			if (proj == null)
				if (throwExceptionOnNotFound)
					throw new HGException("Dimension " + dimPath[j] +
							" does not exist in type " + tinstance);
				else
					return null;
			value = proj.project(value);
			type = proj.getType();
		}
		return new HGTypedValue(value, type);
	}
	
	// ------------------------------------------------------------------------
	// The following part deals solely with the management of circular references
	// during storage and retrieval of aggregate values. Type implementations
	// would normally use those utility methods to track circular references.
	//
	// The implementation is straightforward: two maps and a set are maintained:
	//
	// - JAVA_REF_MAP maps pure Java references (Objects) to HGPersistentHandles.
	//   It is used during storage of aggregate values. It is attached to the 
	//   current transaction as an attribute.
	//
	// - HANDLE_REF_MAP maps HGPersistentHandles to pure Java references. It is
	//   attached to the current thread (since reads are not necessarily enclosed 
	//   in a transaction) and it is used during retrieval of aggregate values.
	//
	// - HANDLE_REF_SET contains the set of value handles that are being released.
	//   It is attached to the current transaction as an attribute.
	//
	// In all cases, type implementations are expected to consult the relevant
	// map in order to see whether the operation (read, write, remove) has already been
	// performed in the current context (transaction write or thread-local read).
	//
	// Obviously, this mechanism doesn't cross thread boundaries. As always, 
	// this limitation will be reexamined as the need arises.
	//
	// Another note: The duality of Record-JavaBeans forces a little hack. We really
	// want to associate the Java bean instance (and not the temporary Record created
	// just so that it is stored by the RecordType implementation) to the 
	// HGPersistentHandle of the value. This information, that we are actually tracking
	// a bean reference, not a Record reference, must be passed from the wrapping type
	// (a JavaBeanBinding) to the underlying type (RecordType) to finally the 
	// TypeUtils.getNewHandleFor method. The interface WrappedRuntimeInstance was
	// created to solve this issue. Couldn't find a more elegant way that doesn't
	// involve changing the HGAtomType interface. The problem is that it is the
	// responsibility of the HGAtomType.store method to create the handle of the 
	// value. But RecordType.store is called with a Record instance, not a bean
	// instance and therefore cannot do the mapping Object->HGPersistentHandle 
	// that we need to track circular references. Got it?
	//
	//
	// [Borislav Iordanov]
	//
	
	private static final String JAVA_REF_MAP = "JAVA_REF_VALUE_MAP".intern();
	private static final String HANDLE_REF_SET = "HANDLE_REF_SET".intern();
	private static final String HANDLE_REF_MAP = "HANDLE_REF_MAP".intern();
/*	private static ThreadLocal<Map<HGPersistentHandle, Object>> HANDLE_REF_MAP = 
		new ThreadLocal<Map<HGPersistentHandle, Object>>(); */
	
	public interface WrappedRuntimeInstance { Object getRealInstance(); }
	
	public static Map<Object, HGPersistentHandle> getTransactionObjectRefMap(HyperGraph hg)
	{
		Map<Object, HGPersistentHandle> refMap = 
			(Map<Object, HGPersistentHandle>)hg.getTransactionManager().getContext().getCurrent().getAttribute(JAVA_REF_MAP);
		if (refMap == null)
		{
			refMap = new IdentityHashMap<Object, HGPersistentHandle>();
			hg.getTransactionManager().getContext().getCurrent().setAttribute(JAVA_REF_MAP, refMap);
		}
		return refMap;
	}
	
	public static Map<HGPersistentHandle, Object> getThreadHandleRefMap(HyperGraph graph)
	{
		Map<HGPersistentHandle, Object> refMap =  
			(Map<HGPersistentHandle, Object>)graph.getTransactionManager().getContext().getCurrent().getAttribute(HANDLE_REF_MAP);
			// HANDLE_REF_MAP.get(); 
		if (refMap == null)
		{
			refMap = new HashMap<HGPersistentHandle, Object>();
			graph.getTransactionManager().getContext().getCurrent().setAttribute(HANDLE_REF_MAP, refMap);			
		}
		return refMap;
	}
	
	public static Set<HGPersistentHandle> getTransactionHandleSet(HyperGraph hg)
	{
		Set<HGPersistentHandle> set =
			(Set<HGPersistentHandle>)hg.getTransactionManager().getContext().getCurrent().getAttribute(HANDLE_REF_SET);
		if (set == null)
		{
			set = new HashSet<HGPersistentHandle>();
			hg.getTransactionManager().getContext().getCurrent().setAttribute(HANDLE_REF_SET, set);
		}
		return set;
	}
	
	public static HGPersistentHandle getNewHandleFor(HyperGraph hg, Object value)
	{
		HGPersistentHandle result = HGHandleFactory.makeHandle();
		if (value instanceof WrappedRuntimeInstance) 
			value = ((WrappedRuntimeInstance)value).getRealInstance();
		getTransactionObjectRefMap(hg).put(value, result);
		return result;
	}
	
	public static boolean isValueReleased(HyperGraph graph, HGPersistentHandle h)
	{
		return getTransactionHandleSet(graph).contains(h);
	}
	
	public static void releaseValue(HyperGraph graph, HGAtomType type, HGPersistentHandle h)
	{
	    // If this was previously added in the course of the current transaction,
	    // remove it from the relevant maps.
		Object instance =  getThreadHandleRefMap(graph).remove(h);
		if (instance != null)
			getTransactionObjectRefMap(graph).remove(instance);
		if (! (type instanceof HGRefCountedType))
		    getTransactionHandleSet(graph).add(h);
	}
	
	public static HGPersistentHandle storeValue(HyperGraph graph, Object value, HGAtomType type)
	{
		Map<Object, HGPersistentHandle> refMap = getTransactionObjectRefMap(graph);
		HGPersistentHandle result = null;
		if (! (type instanceof HGRefCountedType))
		    result = refMap.get(value);
		if (result == null)
		{
			result = type.store(value);
			refMap.put(value, result);
		}
		getThreadHandleRefMap(graph).put(result, value);
		return result;
	}
	
	public static void setValueFor(HyperGraph graph, HGPersistentHandle h, Object value)
	{
		Map<HGPersistentHandle, Object> refMap = getThreadHandleRefMap(graph);
		if (!refMap.containsKey(h))
			refMap.put(h, value);
	}
	
	public static Object makeValue(HyperGraph graph, HGPersistentHandle h, HGAtomType type)
	{
		Map<HGPersistentHandle, Object> refMap = getThreadHandleRefMap(graph); 
		Object result = refMap.get(h);
		if (result == null)
		{
			result = type.make(h, null, null);
			refMap.put(h, result);
		}
		return result;
	}
	
	public static List<HGHandle> subsumesClosure(HyperGraph graph, HGHandle baseType)
	{
		DefaultALGenerator alGenerator = new DefaultALGenerator(graph, 
																new AtomTypeCondition(HGSubsumes.class),											
												                null,
												                false,
												                true,
												                false);
		HGDepthFirstTraversal traversal = new HGDepthFirstTraversal(baseType, alGenerator);
		ArrayList<HGHandle> subTypes = new ArrayList<HGHandle>();
		while (traversal.hasNext())
			subTypes.add(traversal.next().getSecond());
		subTypes.add(baseType);
		return subTypes;
	}
	
	public static boolean deleteType(HyperGraph graph, HGHandle th, boolean recursive)
	{
        if (recursive)
        {
            List<HGHandle> L = subsumesClosure(graph, th);
            for (HGHandle h : L)
                if (!deleteType(graph, h))
                        return false;
            return true;
        }
        else
            return deleteType(graph, th);	    
	}
	
	public static boolean deleteType(HyperGraph graph, Class<?> type, boolean recursive)
	{
		HGHandle th = graph.getTypeSystem().getTypeHandleIfDefined(type);
		if (th == null)
			return true;
		else
		    return deleteType(graph, th, recursive);
	}
	
	public static boolean deleteType(HyperGraph graph, HGHandle type)
	{
		if (deleteInstances(graph, type))
			return graph.remove(type);
		else
			return false;
	}
	
	public static boolean deleteInstances(HyperGraph graph, HGHandle type)
	{		
		int batchSize = 100; // perhaps this should become a parameter
		HGHandle [] batch = new HGHandle[batchSize];
		for (boolean done = false; !done; )
		{
			HGSearchResult<HGHandle> rs = null;
			try
			{
				int fetched = 0;
				graph.getTransactionManager().beginTransaction();
				try
				{
					rs = graph.find(new TypePlusCondition(type));					
					while (fetched < batch.length && rs.hasNext())
						batch[fetched++] = rs.next();
					rs.close();
				}
				finally
				{
					try { graph.getTransactionManager().endTransaction(true); }
					catch (HGTransactionException tex) { throw new HGException(tex); }
				}
				done = fetched == 0;
				graph.getTransactionManager().beginTransaction();
				try
				{
					while (--fetched >= 0)
						if (!graph.remove(batch[fetched]))
							return false;
					graph.getTransactionManager().endTransaction(true);
				}
				catch (Throwable t)
				{
					try { graph.getTransactionManager().endTransaction(false); }
					catch (Throwable t1) { t1.printStackTrace(System.err); }
					done = false;
					HGUtils.wrapAndRethrow(t);
				}
			}
			finally
			{
				HGUtils.closeNoException(rs);
			}
		}
		return true;
	}
}