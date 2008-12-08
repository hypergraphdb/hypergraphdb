package org.hypergraphdb.indexing;

import java.util.Comparator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.storage.ByteArrayConverter;

/**
 * 
 * <p>
 * A <code>LinkIndexer</code> indexes atoms by their target <b>ordered</b> set. That is,
 * all targets, in order, are taken to form the key of the index.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class LinkIndexer extends HGIndexer
{	
	public LinkIndexer()
	{		
	}
	
	public LinkIndexer(HGHandle type)
	{
		super(type);
	}
	
	@Override
	public boolean equals(Object other)
	{
		if (other == this)
			return true;
		if (! (other instanceof LinkIndexer))
			return false;
		LinkIndexer idx = (LinkIndexer)other;
		return getType().equals(idx.getType());
	}

	@Override
	public int hashCode()
	{
		return getType().hashCode();
	}	

	@Override
	public Comparator<?> getComparator(HyperGraph graph)
	{
		return null; // use default byte-by-byte comparator
	}

	@Override
	public ByteArrayConverter<?> getConverter(HyperGraph graph)
	{
		return BAtoBA.getInstance();
	}

	@Override
	public Object getKey(HyperGraph graph, Object atom)
	{
		HGLink link = (HGLink)atom;
		byte [] result = new byte[16*link.getArity()];
		for (int i = 0; i < link.getArity(); i++)
		{
			byte [] src = graph.getPersistentHandle(link.getTargetAt(i)).toByteArray();
			System.arraycopy(src, 0, result, i*16, 16);
		}
		return result;
	}
}
