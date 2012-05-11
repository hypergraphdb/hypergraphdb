package org.hypergraphdb.util;

public interface Var<T> extends Ref<T>
{
	T get();
	void set(T x);
}