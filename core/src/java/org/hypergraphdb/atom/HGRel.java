package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * Represents a name relationship/link between entities.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGRel extends HGPlainLink 
{
	private String name;
	
	public HGRel(String name, HGHandle [] targetSet)
	{
		super(targetSet);
		this.name = name;
	}

	public String getName() 
	{
		return name;
	}
	
	public String toString()
	{
		return name + "[" + getArity() + "]";
	}
}