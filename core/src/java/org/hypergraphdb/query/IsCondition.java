package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Ref;

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
	private Ref<HGHandle> atomHandle;
	
	public IsCondition()
	{		
	}
	
	public IsCondition(Ref<HGHandle> atomHandle)
	{
		this.atomHandle = atomHandle;
	}
	
	public IsCondition(HGHandle atomHandle)
	{
		this.atomHandle = hg.constant(atomHandle);
	}
		
	public Ref<HGHandle> getAtomHandleReference()
	{
		return atomHandle;
	}
	
	public void setAtomHandleReference(Ref<HGHandle> atomHandle)
	{
		this.atomHandle = atomHandle;
	}
	
	public HGHandle getAtomHandle()
	{
		return atomHandle.get();
	}

	public void setAtomHandle(HGHandle atomHandle)
	{
		this.atomHandle = hg.constant(atomHandle);
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
		result = prime * result + ((atomHandle == null || atomHandle.get() == null) ? 0 : atomHandle.get().hashCode());
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
		return HGUtils.eq(atomHandle.get(), other.atomHandle.get());
	}	
}