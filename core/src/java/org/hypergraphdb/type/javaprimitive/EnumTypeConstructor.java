package org.hypergraphdb.type.javaprimitive;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGAtomTypeBase;
import org.hypergraphdb.util.HGUtils;

/**
 * 
 * <p>
 * Manages instances of <code>EnumType</code>. Each <code>EnumType</code> manages
 * a concrete Java <code>Enum</code>.
 * </p>
 *
 * <p>
 * To allow for cross-platform interoperability of enums, the list of symbolic 
 * constants is also recorded along with the fully-qualified Java class name 
 * of the enum. The Java HyperGraph implementation will only use the latter, though.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class EnumTypeConstructor extends  HGAtomTypeBase
{
    public static final HGPersistentHandle HGHANDLE =
        HGHandleFactory.makeHandle("4e3c44ec-da21-11db-84d5-cf67a5f089dc");
 
    @SuppressWarnings("unchecked")
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
	{
		EnumType result = new EnumType();
		result.setHyperGraph(graph);
		HGPersistentHandle [] layout = graph.getStore().getLink(handle);
		HGAtomType stringType = graph.getTypeSystem().getAtomType(String.class);
		String classname = (String)stringType.make(layout[0], null, null);
		try
		{
			Class<Enum> cl = HGUtils.loadClass(getHyperGraph(), classname);
			result.setEnumType(cl);
		}
		catch (ClassNotFoundException ex)
		{
			throw new HGException("Unable to load enum class " + classname, ex);
		}
		return result;
	}

	public HGPersistentHandle store(Object instance)
	{
		EnumType et = (EnumType)instance;
		if (!et.getEnumType().isEnum())
			throw new HGException("Attempting to store non Enum class " + 
								  et.getEnumType().getClass() + " as an enum.");		
		HGPersistentHandle [] layout = new HGPersistentHandle[1 + et.getEnumType().getEnumConstants().length];
		HGAtomType stringType = graph.getTypeSystem().getAtomType(String.class);
		layout[0] = stringType.store(et.getEnumType().getName());
		Enum<?> [] constants =(Enum[])et.getEnumType().getEnumConstants(); 
		for (int i = 0; i < constants.length; i++ )
		{
			layout[i + 1] = stringType.store(constants[i].name());
		}
		return graph.getStore().store(layout);
	}

	public void release(HGPersistentHandle handle)
	{
		HGPersistentHandle [] layout = graph.getStore().getLink(handle);
		HGAtomType stringType = graph.getTypeSystem().getAtomType(String.class);
		for (HGPersistentHandle h : layout)
			stringType.release(h);
		graph.getStore().removeLink(handle);
	}
}