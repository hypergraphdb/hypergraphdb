package org.hypergraphdb;

/**
 * 
 * <p>
 * The interface is for atoms that need to hold a reference to the
 * <code>HyperGraph</code> to which they belong. If an object implements
 * this interface, its <code>setHyperGraph</code> method will be called
 * every time it is read from permanent storage.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public interface HGGraphHolder
{
	/**
	 * <p>During load time, set the <code>HyperGraph</code> 
	 * instance to which this atom belongs.</p>
	 * @param hg The <code>HyperGraph</code> that just loaded
	 * the atom.
	 */
	void setHyperGraph(HyperGraph hg);
}