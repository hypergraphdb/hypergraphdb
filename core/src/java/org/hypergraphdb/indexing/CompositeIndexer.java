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
	private HGHandle [] indexerPartHandles;
	private HGIndexer [] indexerParts = null;
	
	private void fetchIndexers(HyperGraph graph)
	{
		if (indexerParts != null)
			return;
		this.indexerParts = new HGIndexer[indexerParts.length];		
		for (int i = 0; i < indexerPartHandles.length; i++)
			indexerParts[i] = graph.get(indexerPartHandles[i]);
	}
	
	public CompositeIndexer()
	{		
	}
	
	public CompositeIndexer(HGHandle type, HGHandle [] indexerParts)
	{
		super(type);
		this.indexerPartHandles = indexerParts;
	}
	
	@Override
	public boolean equals(Object other)
	{
		if (! (other instanceof CompositeIndexer))
			return false;
		return HGUtils.eq(indexerPartHandles, ((CompositeIndexer)other).indexerPartHandles);
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
		fetchIndexers(graph);
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
		int x = indexerPartHandles.length;
		for (HGHandle h : indexerPartHandles)
			x ^= h.hashCode() >> 16;
		return x;
	}
}