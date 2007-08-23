package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;

/**
 * 
 * <p>
 * This is a <code>HGQueryCondition</code> that contrains the value of an
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
	private Class javaClass;
	private HGHandle typeHandle;

	protected boolean satisfies(HyperGraph hg, HGHandle atomHandle, Object atom, HGHandle type)
	{		
		return type.equals(typeHandle) && super.satisfies(hg, atomHandle, atom, type);
	}

	public TypedValueCondition(HGHandle typeHandle, Object value)
	{
		this(typeHandle, value, ComparisonOperator.EQ);
	}
	
	public TypedValueCondition(Class javaClass, Object value)
	{
		this(javaClass, value, ComparisonOperator.EQ);
	}
	
	public TypedValueCondition(HGHandle typeHandle, Object value, ComparisonOperator op)
	{
		super(value, op);
		this.typeHandle = typeHandle;
	}

	public TypedValueCondition(Class javaClass, Object value, ComparisonOperator op)
	{
		super(value, op);
		this.javaClass = javaClass;
	}
	
	public HGHandle getTypeHandle()
	{
		return typeHandle;
	}
	
	public Class getJavaClass()
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
}