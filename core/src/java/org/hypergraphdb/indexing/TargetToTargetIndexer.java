package org.hypergraphdb.indexing;

import java.util.Comparator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.HGUtils;

public class TargetToTargetIndexer extends HGValueIndexer
{
	private int fromTarget, toTarget;
	
	public TargetToTargetIndexer()
	{		
	}
	
	public TargetToTargetIndexer(HGHandle type, int fromTarget, int toTarget)
	{
		super(type);
		this.fromTarget = fromTarget;
		this.toTarget = toTarget;
	}
		
	public int getFromTarget()
	{
		return fromTarget;
	}

	public void setFromTarget(int fromTarget)
	{
		this.fromTarget = fromTarget;
	}

	public int getToTarget()
	{
		return toTarget;
	}

	public void setToTarget(int toTarget)
	{
		this.toTarget = toTarget;
	}

	@Override
	public Object getKey(HyperGraph graph, Object atom)
	{
		return ((HGLink)atom).getTargetAt(fromTarget);
	}
	
	@Override
	public Object getValue(HyperGraph graph, Object atom)
	{
		return ((HGLink)atom).getTargetAt(toTarget);
	}

	public ByteArrayConverter<HGHandle> getValueConverter(final HyperGraph graph)
	{
		return new ByteArrayConverter<HGHandle>()
		{
			public byte[] toByteArray(HGHandle h)
			{
				return graph.getPersistentHandle(h).toByteArray();
			}
			
			public HGHandle fromByteArray(byte [] A)
			{
				return HGHandleFactory.makeHandle(A);
			}
		};
	}
	
	@Override
	public Comparator<?> getComparator(HyperGraph graph)
	{
		return null;
	}

	@Override
	public ByteArrayConverter<?> getConverter(final HyperGraph graph)
	{
		return new ByteArrayConverter<HGHandle>()
		{
			public byte[] toByteArray(HGHandle h)
			{
				return graph.getPersistentHandle(h).toByteArray();
			}
			
			public HGHandle fromByteArray(byte [] A)
			{
				return HGHandleFactory.makeHandle(A);
			}
		};
	}

	@Override
	public int hashCode()
	{
		return HGUtils.hashThem(getType(), HGUtils.hashThem(fromTarget, toTarget));
	}
	
	@Override
	public boolean equals(Object other)
	{
		if (! (other instanceof TargetToTargetIndexer))
			return false;
		TargetToTargetIndexer i = (TargetToTargetIndexer)other;
		return HGUtils.eq(getType(), i.getType()) && 
			   fromTarget == i.fromTarget &&
			   toTarget == i.toTarget;
	}	
}