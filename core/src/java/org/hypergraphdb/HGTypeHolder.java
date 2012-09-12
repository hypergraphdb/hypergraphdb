package org.hypergraphdb;

import org.hypergraphdb.annotation.HGIgnore;

/**
 * 
 * <p>
 * Implement this interface for atoms that will hold the instance of their HyperGraphDB 
 * type as a bean property. The system will set this property when an atom is added 
 * to or loaded from the database.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <T>
 */
public interface HGTypeHolder<T>
{
	@HGIgnore
	T getAtomType();
	@HGIgnore
	void setAtomType(T atomType);
}