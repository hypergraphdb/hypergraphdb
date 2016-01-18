package org.hypergraphdb.util;

@FunctionalInterface
public interface Ref<T>
{
	T get();
}