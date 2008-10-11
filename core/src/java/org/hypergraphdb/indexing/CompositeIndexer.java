package org.hypergraphdb.indexing;

import java.util.Comparator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.HGUtils;

@SuppressWarnings("unchecked")
public class CompositeIndexer extends HGIndexer
{
	private HGIndexer [] indexerParts = null;
	
	
	public CompositeIndexer()
	{		
	}
		
	public CompositeIndexer(HGHandle type, HGIndexer [] indexerParts)
	{
		super(type);
		if (indexerParts == null || indexerParts.length == 0)
			throw new IllegalArgumentException("Attempt to construct CompositeIndexer with null or empty parts.");		
		this.indexerParts = indexerParts;
	}
	
	@Override
	public boolean equals(Object other)
	{
		if (! (other instanceof CompositeIndexer))
			return false;
		return HGUtils.eq(indexerParts, ((CompositeIndexer)other).indexerParts);
	}

	@Override
	public Comparator<?> getComparator(HyperGraph graph)
	{
		return null;
	}

	@Override
	public ByteArrayConverter<?> getConverter(HyperGraph graph)
	{
		return BAtoBA.getInstance();
	}

	@Override
	public Object getKey(HyperGraph graph, Object atom)
	{
		byte [][] keys = new byte[indexerParts.length][];
		int size = 1;
		for (int i = 0; i < indexerParts.length; i++)
		{
			HGIndexer ind = indexerParts[i];
			Object key = ind.getKey(graph, atom);
			keys[i] = ((ByteArrayConverter<Object>)ind.getConverter(graph)).toByteArray(key);			
			size += keys[i].length + 4;
		}
		byte [] B = new byte[size];
		B[1] = (byte)keys.length;
		int pos = 1;
		for (byte [] curr : keys)
		{
			BAUtils.writeInt(curr.length, B, pos);
			pos += 4;
			System.arraycopy(curr, 0, B, pos, curr.length);
			pos += curr.length;
		}
		return B;
	}

	@Override
	public int hashCode()
	{
		if (indexerParts == null) return 0;
		int x = indexerParts.length;
		for (HGIndexer ind : indexerParts)
			x ^= ind.hashCode() >> 16;
		return x;
	}

	public HGIndexer[] getIndexerParts()
	{
		return indexerParts;
	}

	public void setIndexerParts(HGIndexer[] indexerParts)
	{
		this.indexerParts = indexerParts;
	}	
}