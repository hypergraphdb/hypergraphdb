package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGUtils;

/**
 * 
 * <p>
 * This is a <code>HGQueryCondition</code> that constrains the value of an
 * atom as well as its type. In general, the HyperGraph type system allows
 * values to have multiple types. Two atoms <code>X</code> and
 * <code>Y</code> can share the same value <code>V</code> without however
 * having the same type. <code>X</code> may be of type <code>T</code> and 
 * <code>Y</code> may be of type <code>S</code> different than <code>T</code>.
 * In such situations, when performing a query by value, a <code>TypedValueCondition<code>
 * should be used instead of <code>AtomValueCondition</code>. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class TypedValueCondition extends AtomValueCondition
{
	private Class<?> javaClass;
	private HGHandle typeHandle;

	public TypedValueCondition()
	{
		
	}
	public TypedValueCondition(HGHandle typeHandle, Object value)
	{
		this(typeHandle, value, ComparisonOperator.EQ);
	}
	
	public TypedValueCondition(Class<?> javaClass, Object value)
	{
		this(javaClass, value, ComparisonOperator.EQ);
	}
	
	public TypedValueCondition(HGHandle typeHandle, Object value, ComparisonOperator op)
	{
		super(value, op);
		this.typeHandle = typeHandle;
	}

	public TypedValueCondition(Class<?> javaClass, Object value, ComparisonOperator op)
	{
		super(value, op);
		this.javaClass = javaClass;
	}
	
	public boolean satisfies(HyperGraph hg, HGHandle handle) 
	{
		Object atom = null;
		atom = hg.get(handle);		
		if (atom == null)
			return false;
		HGHandle type = hg.getTypeSystem().getTypeHandle(handle);
		return type.equals(typeHandle) && compareToValue(hg, atom); 
	}
		
	public void setTypeHandle(HGHandle typeHandle)
	{
		this.typeHandle = typeHandle;
	}
	
	public HGHandle getTypeHandle()
	{
		return typeHandle;
	}
	
	public void setJavaClass(Class<?> javaClass)
	{
		this.javaClass = javaClass;
	}
	
	public Class<?> getJavaClass()
	{
		return javaClass;
	}
	
	public String toString()
	{
		StringBuffer result = new StringBuffer("valueIs(");
		result.append(getOperator());
		result.append(",");
		result.append(String.valueOf(getValue()));
		result.append(" with type ");
		result.append(typeHandle);
		result.append(")");
		return result.toString();
	}
	
	public int hashCode() 
	{ 
		return  super.hashCode() + 
				(javaClass == null ? typeHandle.hashCode() : javaClass.hashCode());  
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof TypedValueCondition))
			return false;
		else
		{
			TypedValueCondition c = (TypedValueCondition)x;
			return (javaClass == null ? 
					HGUtils.eq(typeHandle, c.typeHandle) : 
					HGUtils.eq(javaClass, c.javaClass)) &&
					super.equals(x);
		}
	}	
}