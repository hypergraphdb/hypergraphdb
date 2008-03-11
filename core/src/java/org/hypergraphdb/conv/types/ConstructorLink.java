package org.hypergraphdb.conv.types;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;

public class ConstructorLink extends HGPlainLink
{

	public ConstructorLink(HGHandle [] link)
	{
		super(link);		
		if (link.length < 1)
			throw new IllegalArgumentException("The HGHandle [] passed to the ConstructorLink constructor must be at least of length 1.");
	}
	
	public String toString()
	{
		StringBuffer result = new StringBuffer();
		result.append("ConstructorLink(");
		//result.append(getParent());
		result.append(",");
		result.append(getArity());
		result.append(")");
		return result.toString();
	}

}

