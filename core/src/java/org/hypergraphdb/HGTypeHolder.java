package org.hypergraphdb;

public interface HGTypeHolder<T>
{
	T getAtomType();
	void setAtomType(T atomType);
}