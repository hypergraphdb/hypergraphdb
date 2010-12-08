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
public abstract class HGKeyIndexer implements HGIndexer
{
    private HGHandle type;
    
    public HGKeyIndexer()
    {       
    }
    
    public HGKeyIndexer(HGHandle type)
    {
        this.type = type;
    }

    public HGHandle getType()
    {
        return type;
    }

    public void setType(HGHandle type)
    {
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    public void index(HyperGraph graph, HGPersistentHandle atomHandle, Object atom, HGIndex index)
    {
        index.addEntry(getKey(graph, atom), atomHandle);        
    }
    
    @SuppressWarnings("unchecked")
    public void unindex(HyperGraph graph, HGPersistentHandle atomHandle, Object atom, HGIndex index)
    {
        index.removeEntry(getKey(graph, atom), atomHandle);        
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
    public abstract Object getKey(HyperGraph graph, Object atom);
}