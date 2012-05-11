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
	
	@SuppressWarnings("unchecked")
	public <T> Var<T> get(final String name)
	{		
		return new Var<T>()
		{	
			public T get() { return (T)vars.get(name); }
			public void set(T value) { vars.put(name, value); }
		};
	}	
}