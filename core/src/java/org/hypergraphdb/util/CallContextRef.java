package org.hypergraphdb.util;

import java.util.LinkedList;

public abstract class CallContextRef<T> implements Ref<T>
{
	ThreadLocal<LinkedList<T>> stack = new ThreadLocal<LinkedList<T>>() {
      @Override
      protected LinkedList<T> initialValue()
      {
          return new LinkedList<T>();
      }		
	};
	
	public abstract T compute();
	
	public void push() { stack.get().push(compute()); }
	public void pop() { stack.get().pop(); }
	
	@Override
	public T get()
	{
		return stack.get().peek();
	}
}