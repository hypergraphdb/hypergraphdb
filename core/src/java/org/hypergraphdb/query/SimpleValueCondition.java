/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.ReadyRef;
import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * Basic class for conditions examining individual primitive values.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public abstract class SimpleValueCondition implements HGQueryCondition, HGAtomPredicate 
{
	protected Object value;
	protected ComparisonOperator operator;
 	
	/**
	 * We are using the standard Java <code>java.lang.Comparable</code> interface here
	 * on the <code>value</code> parameter. The passed in type is ignored, but is
	 * part of the method signature because it is still not clear how exactly
	 * value orderings are to be treated. All Java primitive types are comparable and
	 * our primitive types are nothing more than the Java primitive types. Perhaps we
	 * need an "ordered" interface for atom types. Of course values can always implement
	 * the java.lang.Comparable, but this breaks HyperGraph paradigm where such semantics
	 * are defined by the types, not their values.
	 */
	protected boolean compareToValue(HyperGraph graph, Object x, HGHandle type)
	{
		if (x instanceof HGValueLink)
			x = ((HGValueLink)x).getValue();
    	switch (operator)
    	{
    		case EQ:
    			return value.equals(x);
    		case LT:
    			return ((Comparable)value).compareTo(x) < 0;
    		case GT:
    			return ((Comparable)value).compareTo(x) > 0;
    		case LTE:
    			return ((Comparable)value).compareTo(x) <= 0;
    		case GTE:
    			return ((Comparable)value).compareTo(x) >= 0;   
    		default:
    			throw new HGException("Wrong operator code [" + operator + "] passed to SimpleValueCondition.");
    	}
 	}
	
	protected abstract boolean satisfies(HyperGraph hg, 
								 		 HGHandle atomHandle,
								 		 Object atom,
								 		 HGHandle type);
	
	public SimpleValueCondition(Object value)
	{
		this.value = value;
        this.operator = ComparisonOperator.EQ;
	}

    public SimpleValueCondition(Object value, ComparisonOperator operator)
    {
        this.value = value;
        this.operator = operator;
    }
    
    public Object getValue()
    {
        return value;
    }
    
    public ComparisonOperator getOperator()
    {
        return operator;
    }

	public boolean satisfies(HyperGraph hg, HGHandle handle) 
	{
		HGHandle type = null;
		Object atom = null;
		if (hg.isLoaded(handle))
		{
			atom = hg.get(handle);
			type = hg.getTypeSystem().getTypeHandle(handle);			
		}
		else
		{
			HGPersistentHandle [] layout = hg.getStore().getLink((HGPersistentHandle)handle);
            HGHandle [] targets = new HGHandle[layout.length - 2];
            for (int i = 2; i < layout.length; i++)
            	targets[i-2] = layout[i];
			type = layout[0];
			TypeUtils.initiateAtomConstruction(hg, layout[1]);
			atom = hg.getTypeSystem().getType(type).make(layout[1], 
														 new ReadyRef<HGHandle[]>(targets), 
														 null);
			TypeUtils.atomConstructionComplete(hg, layout[1]);
		}
		
		if (atom == null)
			return false;
		else
			return satisfies(hg, handle, atom, type);
	}
	
	public int hashCode()
	{
		return HGUtils.hashIt(value);
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof SimpleValueCondition))
			return false;
		SimpleValueCondition y = (SimpleValueCondition)x;
		return HGUtils.eq(value, y.value) && operator.equals(y.operator);
	}
}