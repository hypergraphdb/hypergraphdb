package org.hypergraphdb;

/**
 * 
 * <p>
 * Implement this interface for atoms that will hold their HyperGraphDB handle
 * as a bean property.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public interface HGHandleHolder
{
	/**
	 * Return the HyperGraphDB atom handle of this object. 
	 */
	HGHandle getAtomHandle();
	
	/**
	 * <p>
	 * Set the HyperGraphDB atom handle of this object - used normally only
	 * by the {@link HyperGraph} instance holding this atom.
	 * </p>
	 * 
	 * @param handle The atom handle.
	 */
	void setAtomHandle(HGHandle handle);
}