package org.hypergraphdb.indexing;

import java.util.Comparator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.ByteArrayConverter;

/**
 * 
 * <p>
 * An <code>HGIndexer</code> represents an atom used internally
 * by HyperGraphDB to manage indices. All indexers apply to a 
 * specific atom types. All indexers are responsible for producing
 * a key for the given atom instance being indexed. In addition,
 * when the keys produced are NOT of type <code>byte[]</code>, an indexer
 * must provide a <code>ByteArrayConverter</code> capable of translating
 * a key to/from a <code>byte[]</code>.    
 * </p>
 * <p>
 * Because indexers are stored as HyperGraphDB atoms, they must obey
 * the Java Beans conventions of having a default constructor and getter/setter
 * pair for each property that must be recorded into storage.
 * </p>
 * 
 * <p>
 * Implementations may also provide a <code>Comparator</code> for keys for
 * sorted indices and when the default <code>byte[]</code> comparator is not
 * suitable.
 * </p>
 * 
 * <p>
 * <strong>IMPORTANT</strong>:Instances of <code>HGIndexer</code> are frequently used to perform lookup on
 * existing indices. For example, when trying to determine whether an atom of 
 * a complex type is being indexed by some projection, one constructs a 
 * <code>ByPartIndexer</code> and performs a lookup in the index manager. Thus, it 
 * is essential that implementation of <code>HGIndexer</code> provide proper <code>hashCode</code>
 * and  <code>equals</code> methods.
 * </p>
 * @author Borislav Iordanov
 *
 */
public abstract class HGIndexer
{
	private HGHandle type;
	
	public HGIndexer()
	{		
	}
	
	public HGIndexer(HGHandle type)
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
	
	/**
	 * <p>Return a <code>ByteArrayConverter</code> capable of translating keys
	 * returned by this indexer to/from a <code>byte[]</code>.
	 *  
	 * @param graph The current HyperGraph instance.
	 * @return The <code>ByteArrayConverter</code> for type of index keys 
	 * return by this indexer or <code>null</code> if keys are of type <code>byte[]</code>. 
	 */
	public abstract ByteArrayConverter<?> getConverter(HyperGraph graph);
	
	/**
	 * <p>
	 * Return a comparator used to compare key values return by this indexer.
	 * Note that the comparator's <code>compare</code> method will be invoked 
	 * with <code>byte[]</code> parameters. It is the comparator's responsibility
	 * to convert them to the appropriate run-time type for performing the comparison
	 * if need be.
	 * </p>
	 * 
	 * <p>
	 * The method may return <code>null</code> if a default byte-by-byte comparator is to be
	 * used.
	 * </p>
	 * 
	 * @param graph The current HyperGraph instance.
	 * @return A comparator used to compare key values return by this indexer or 
	 * <code>null</code> to use a default byte-by-byte comparison of keys. 
	 */
	public abstract Comparator<?> getComparator(HyperGraph graph);
	
	/**
	 * <p>Declared to enforce implementation.</p> 
	 */
	public abstract int hashCode();
	
	/**
	 * <p>Declared to enforce implementation.</p> 
	 */
	public abstract boolean equals(Object other);
}