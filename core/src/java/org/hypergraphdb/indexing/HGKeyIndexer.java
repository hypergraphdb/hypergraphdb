package org.hypergraphdb.indexing;

import org.hypergraphdb.HGHandle;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;

/**
 * 
 * <p>
 * An <code>HGKeyIndexer</code> represents an atom used internally
 * by HyperGraphDB to manage key-based indices. All such indexers are 
 * responsible for producing
 * a key for the given atom instance being indexed. In addition,
 * when the keys produced are NOT of type <code>byte[]</code>, a key indexer
 * must provide a <code>ByteArrayConverter</code> capable of translating
 * a key to/from a <code>byte[]</code>.    
 * </p>
 * 
 * <p>
 * Implementations may also provide a <code>Comparator</code> for keys for
 * sorted indices and when the default <code>byte[]</code> comparator is not
 * suitable.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public abstract class HGKeyIndexer<KeyType> implements HGIndexer<KeyType, HGPersistentHandle>
{
  	private String name = null;
    private HGHandle type;
    
    public HGKeyIndexer()
    {       
    }
    
    public HGKeyIndexer(String name, HGHandle type)
    {
      	this.name = name;
        this.type = type;
    }

    public HGKeyIndexer(HGHandle type)
    {
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public HGHandle getType()
    {
        return type;
    }

    public void setType(HGHandle type)
    {
        this.type = type;
    }

	public void index(HyperGraph graph, HGHandle atomHandle, Object atom, HGIndex<KeyType, HGPersistentHandle> index)
    {
        index.addEntry(getKey(graph, atom), atomHandle.getPersistent());        
    }
    
	public void unindex(HyperGraph graph, HGHandle atomHandle, Object atom, HGIndex<KeyType, HGPersistentHandle> index)
    {
        index.removeEntry(getKey(graph, atom), atomHandle.getPersistent());        
    }
    
    /**
     * <p>
     * Returns an index key for the given atom.
     * </p>
     * 
     * @param graph The current <code>HyperGraph</code> instance.
     * @param atom The atom being indexed.
     * @return The index key. If the return value is not a <code>byte[]</code>, 
     * a non-null <code>ByteArrayConverter</code> must be provided by the
     * <code>getConverter</code> method.
     */
    public abstract KeyType getKey(HyperGraph graph, Object atom);
}