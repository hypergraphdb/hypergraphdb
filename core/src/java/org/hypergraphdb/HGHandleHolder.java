package org.hypergraphdb;

import org.hypergraphdb.annotation.HGIgnore;

/**
 * 
 * <p>
 * Implement this interface for atoms that will hold their HyperGraphDB handle
 * as a bean property. The system will set this property when an atom is added 
 * to or loaded from the database.
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
	@HGIgnore
	HGHandle getAtomHandle();
	
	/**
	 * <p>
	 * Set the HyperGraphDB atom handle of this object - used normally only
	 * by the {@link HyperGraph} instance holding this atom.
	 * </p>
	 * 
	 * @param handle The atom handle.
	 */
	@HGIgnore
	void setAtomHandle(HGHandle handle);
}