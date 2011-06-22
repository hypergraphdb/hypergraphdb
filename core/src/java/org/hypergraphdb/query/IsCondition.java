package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;

/**
 * <p>
 * An "identity" condition that evaluates to true for a specific handle. It
 * translates to a result set containing the specified atom handle.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class IsCondition implements HGQueryCondition, HGAtomPredicate
{
	private HGHandle atomHandle;
	
	public IsCondition()
	{		
	}
	
	public IsCondition(HGHandle atomHandle)
	{
		this.atomHandle = atomHandle;
	}
		
	public HGHandle getAtomHandle()
	{
		return atomHandle;
	}

	public void setAtomHandle(HGHandle atomHandle)
	{
		this.atomHandle = atomHandle;
	}

	public boolean satisfies(HyperGraph graph, HGHandle handle)
	{
		return atomHandle.equals(handle);
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((atomHandle == null) ? 0 : atomHandle.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IsCondition other = (IsCondition) obj;
		if (atomHandle == null)
		{
			if (other.atomHandle != null)
				return false;
		} else if (!atomHandle.equals(other.atomHandle))
			return false;
		return true;
	}	
}
