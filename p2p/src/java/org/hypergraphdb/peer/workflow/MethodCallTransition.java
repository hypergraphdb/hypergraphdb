/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import org.hypergraphdb.annotation.HGTransact;
import org.hypergraphdb.transaction.HGTransactionConfig;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * A transition that is implemented as a method in the concrete <code>Activity</code>
 * class.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class MethodCallTransition implements Transition
{
    private Method method;
    
    public MethodCallTransition(Method method)
    {
        this.method = method;
    }
    
    public WorkflowStateConstant apply(final Activity activity, final Object... args)
    {
    	HGTransact t = method.getAnnotation(HGTransact.class);
    	Callable<WorkflowStateConstant> call = new Callable<WorkflowStateConstant>() {
    	public WorkflowStateConstant call() throws Exception
    	{
   			return (WorkflowStateConstant)method.invoke(activity, args);
    	}};
    	
        try
        {
        	if (t == null)
        		return call.call();
        	else
        	{
        		HGTransactionConfig txconfig = HGTransactionConfig.DEFAULT;
        		if ("read".equals(t.value()))
        			txconfig = HGTransactionConfig.READONLY;
        		return activity.getThisPeer().getGraph().getTransactionManager().ensureTransaction(call, txconfig);
        	}
        }
        catch (Exception e)
        {
        	System.err.println("Action on transition in activity " + activity + " args:" + args);
        	for (Object x : args) System.err.println(x);
        	e.printStackTrace(System.err);
        	HGUtils.throwRuntimeException(e);
        } 
        return null; // unreachable
    }
    
    public Method getMethod()
    {
        return method;
    }
    
    public String toString()
    {
        return "MethodCallTransition:" + method;
    }
}