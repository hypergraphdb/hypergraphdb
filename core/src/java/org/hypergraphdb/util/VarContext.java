package org.hypergraphdb.util;

import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Stack;

/**
 * <p>
 * This is a glorified name/value map implementing the handling of query variables. 
 * </p>
 * 
 * <p>
 * The glory comes from the implementation of a thread-local (and therefore call stack 
 * bound) stack of frames.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
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
	private ThreadLocal<HashMap<String, Object>> locals = new ThreadLocal<HashMap<String, Object>>() {		
        @Override protected HashMap<String, Object> initialValue() {
            return new HashMap<String, Object>();
    }};
	private class VarImpl<T> implements Var<T>
	{
		String name;
		public VarImpl(String name) { this.name = name; }
		@SuppressWarnings("unchecked")
		public T get() 
		{ 
			T result = (T)locals.get().get(name);
			return result == null ? (T)vars.get(name) : result;
		}
		public void set(T value) 
		{ 
			locals.get().put(name, value); 
		}
        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }
        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            VarImpl<?> other = (VarImpl<?>) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (name == null)
            {
                if (other.name != null)
                    return false;
            }
            else if (!name.equals(other.name))
                return false;
            return true;
        }
        private VarContext getOuterType()
        {
            return VarContext.this;
        }		
	}

	public boolean isSameVar(Var<?> v1, Var<?> v2)
	{
		return ((VarImpl<?>)v1).name.equals(((VarImpl<?>)v2).name);
	}
	
	public <T> Var<T> get(final String name)
	{		
		return new VarImpl<T>(name);
	}
	
	@SuppressWarnings("unchecked")
    public <T> T getGlobalValue(String name)
	{
		return (T)vars.get(name);
	}
	
	public <T> void setGlobalValue(final String name, T value)
	{
		vars.put(name, value);
	}
}