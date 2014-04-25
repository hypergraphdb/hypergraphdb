package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.util.Ref;
import org.hypergraphdb.util.TempLink;

/**
 * <p>
 * A <code>PositionedLinkCondition</code> constraints the query result set to
 * links pointing to a target atom positioned within a predetermined range in 
 * the link tuple. The range is specified with a lower and an upper bound where
 * negative values mean that position are evaluated from the last target in the link.
 * For example a value of -1 means the last target in the link tuple, -2 means the 
 * one before the last etc. In addition, the condition allows you to specify that
 * the target should be position anywhere else except in the specified range. You can 
 * do this by passing <code>true</code> as the <code>complement</code> parameter
 * in the condition's constructor. 
 * </p>
 * 
 * <p>
 * <b>Note</b> that the lower and upper bound in the range specification are inclusive.
 * A range of [1, 5] means all positions 1 to 5, included, are examined. Position counting
 * starts at 0 and, as mentioned above, position -1 means the last element.
 * </p>
 * 
 * @author Alain Picard, Borislav Iordanov
 */
public class PositionedIncidentCondition implements HGQueryCondition, HGAtomPredicate
{
	private Ref<HGHandle> targetRef = null;
	private Ref<Integer> lowerBoundRef, upperBoundRef;
	private Ref<Boolean> complementRef;
	
	public PositionedIncidentCondition()
	{
	}

	public PositionedIncidentCondition(HGHandle target, Integer position)
	{
		this(hg.constant(target), hg.constant(position));
	}

	public PositionedIncidentCondition(HGHandle target, Integer lowerBound, Integer upperBound)
	{
		this(hg.constant(target), hg.constant(lowerBound), hg.constant(upperBound), hg.constant(false));
	}

	public PositionedIncidentCondition(HGHandle target, Integer lowerBound, Integer upperBound, boolean complement)
	{
		this(hg.constant(target), hg.constant(lowerBound), hg.constant(upperBound), hg.constant(complement));
	}
	
	public PositionedIncidentCondition(Ref<HGHandle> target, Ref<Integer> position)
	{
		this(target, position, hg.constant(false));
	}
		
	public PositionedIncidentCondition(Ref<HGHandle> target, 
									   Ref<Integer> position,
									   Ref<Boolean> complement)
	{
		this.targetRef = target;
		this.lowerBoundRef = position;
		this.upperBoundRef = position;
		this.complementRef = complement;
	}
	
	public PositionedIncidentCondition(Ref<HGHandle> target, 
									   Ref<Integer> lowerBound, 
									   Ref<Integer> upperBound,
									   Ref<Boolean> complement)
	{
		this.targetRef = target;
		this.lowerBoundRef = lowerBound;
		this.upperBoundRef = upperBound;
		this.complementRef = complement;
	}
	
	public Ref<HGHandle> getTargetRef()
	{
		return targetRef;
	}

	public void setTargetRef(Ref<HGHandle> targetRef)
	{
		this.targetRef = targetRef;
	}

	public Ref<Integer> getLowerBoundRef()
	{
		return lowerBoundRef;
	}

	public void setLowerBoundRef(Ref<Integer> lowerBoundRef)
	{
		this.lowerBoundRef = lowerBoundRef;
	}

	public Ref<Integer> getUpperBoundRef()
	{
		return upperBoundRef;
	}

	public void setUpperBoundRef(Ref<Integer> upperBoundRef)
	{
		this.upperBoundRef = upperBoundRef;
	}

	public Ref<Boolean> getComplementRef()
	{
		return complementRef;
	}

	public void setComplementRef(Ref<Boolean> complementRef)
	{
		this.complementRef = complementRef;
	}

	public boolean satisfies(HyperGraph hg, HGHandle handle)
	{
		HGPersistentHandle target = targetRef.get().getPersistent();
		HGLink link = null;
		// If the atom corresponding to 'handle' is already in the cache, there
		// is no point
		// fetching it from permanent storage. Otherwise, there's no point
		// caching the actual atom...
		if (hg.isLoaded(handle))
		{
			Object atom = hg.get(handle);
			if (!(atom instanceof HGLink))
				return false;
			link = (HGLink) atom;
		}
		else
		{
			HGPersistentHandle[] A = hg.getStore().getLink(handle.getPersistent());
			if (A == null || A.length <=2)  //this indicates that we don't have a link
			    return false;			
			link = new TempLink(A, 2);
		}
		
		int ub = upperBoundRef.get();
		if (ub < 0)
			ub = link.getArity() + ub;
		int lb = lowerBoundRef.get();
		if (lb < 0)
			lb = link.getArity() + lb;
		
		if (lb > ub 
			|| lb < 0 
			|| ub < 0
			|| lb >= link.getArity()
			|| ub >= link.getArity())
			return false;
		
		if (complementRef.get())
		{
			for (int i = 0; i < lb; i++)
				if (link.getTargetAt(i).equals(target))
					return true;
			for (int i = ub + 1; i < link.getArity(); i++)
				if (link.getTargetAt(i).equals(target))
					return true;
			return false;
		}
		else
		{
			for (int i = lb; i <= ub; i++)
				if (link.getTargetAt(i).equals(target))
					return true;
			return false;
		}
	}

	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((complementRef == null) ? 0 : complementRef.hashCode());
		result = prime * result
				+ ((lowerBoundRef == null) ? 0 : lowerBoundRef.hashCode());
		result = prime * result
				+ ((targetRef == null) ? 0 : targetRef.hashCode());
		result = prime * result
				+ ((upperBoundRef == null) ? 0 : upperBoundRef.hashCode());
		return result;
	}

	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PositionedIncidentCondition other = (PositionedIncidentCondition) obj;
		if (complementRef == null)
		{
			if (other.complementRef != null)
				return false;
		}
		else if (!complementRef.equals(other.complementRef))
			return false;
		if (lowerBoundRef == null)
		{
			if (other.lowerBoundRef != null)
				return false;
		}
		else if (!lowerBoundRef.equals(other.lowerBoundRef))
			return false;
		if (targetRef == null)
		{
			if (other.targetRef != null)
				return false;
		}
		else if (!targetRef.equals(other.targetRef))
			return false;
		if (upperBoundRef == null)
		{
			if (other.upperBoundRef != null)
				return false;
		}
		else if (!upperBoundRef.equals(other.upperBoundRef))
			return false;
		return true;
	}

	public String toString()
	{
		StringBuffer result = new StringBuffer("incidentAt(");
		result.append(targetRef.get().toString());
		result.append(',');
		result.append(lowerBoundRef.get().toString());
		result.append(',');
		result.append(upperBoundRef.get().toString());
		result.append(',');
		result.append(complementRef.get().toString());
		result.append(")");
		return result.toString();
	}
}