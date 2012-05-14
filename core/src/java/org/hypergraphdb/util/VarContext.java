package org.hypergraphdb.util;

import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Stack;

public class VarContext
{
	static ThreadLocal<Stack<VarContext>> stack = new ThreadLocal<Stack<VarContext>>() {
        @Override protected Stack<VarContext> initialValue() {
            return new Stack<VarContext>();
        }
	};

	// Context manipulation
	public static VarContext pushFrame()
	{
		return stack.get().push(new VarContext());
	}

	public static VarContext pushFrame(VarContext ctx)
	{
		return stack.get().push(ctx);
	}
	
	public static VarContext popFrame()
	{
		if (stack.get().isEmpty())
			throw new NoSuchElementException();
		else
			return stack.get().pop();
	}
	
	public static VarContext ctx ()
	{
		if (stack.get().isEmpty())
			return new VarContext();
		else
			return stack.get().peek();
	}
	
	private HashMap<String, Object> vars = new HashMap<String, Object>();
	
	private class VarImpl<T> implements Var<T>
	{
		String name;
		public VarImpl(String name) { this.name = name; }
		@SuppressWarnings("unchecked")
		public T get() { return (T)vars.get(name); }
		public void set(T value) { vars.put(name, value); }		
	}

	public boolean isSameVar(Var<?> v1, Var<?> v2)
	{
		return ((VarImpl<?>)v1).name.equals(((VarImpl<?>)v2).name);
	}
	
	public <T> Var<T> get(final String name)
	{		
		return new VarImpl<T>(name);
	}
}