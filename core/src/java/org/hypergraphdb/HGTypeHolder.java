package org.hypergraphdb;

import org.hypergraphdb.annotation.HGIgnore;

public interface HGTypeHolder<T>
{
	@HGIgnore
	T getAtomType();
	@HGIgnore
	void setAtomType(T atomType);
}