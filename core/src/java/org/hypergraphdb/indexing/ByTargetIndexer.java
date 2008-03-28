package org.hypergraphdb.indexing;

import java.util.Comparator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.ByteArrayConverter;

/**
 * 
 * <p>
 * Represents an index by a specific target position in ordered
 * links.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class ByTargetIndexer extends HGIndexer
{
	private int target;
	
	public ByTargetIndexer()
	{		
	}
	
	public ByTargetIndexer(HGHandle type, int target)
	{
		super(type);
		this.target = target;
	}

	public int getTarget()
	{
		return target;
	}

	public void setTarget(int target)
	{
		this.target = target;
	}


	@Override
	public Comparator getComparator(HyperGraph graph)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteArrayConverter getConverter(HyperGraph graph)
	{
		return BAtoHandle.getInstance();
	}

	@Override
	public Object getKey(HyperGraph graph, Object atom)
	{
		return graph.getPersistentHandle(((HGLink)atom).getTargetAt(target));
	}

	@Override
	public boolean equals(Object other)
	{
		if (other == this)
			return true;
		if (! (other instanceof ByTargetIndexer))
			return false;
		ByTargetIndexer idx = (ByTargetIndexer)other;
		return getType().equals(idx.getType()) && idx.target == target;
	}

	@Override
	public int hashCode()
	{
		return getType().hashCode();
	}	
}