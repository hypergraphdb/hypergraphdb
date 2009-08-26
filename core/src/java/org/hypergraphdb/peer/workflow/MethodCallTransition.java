package org.hypergraphdb.peer.workflow;

import java.lang.reflect.Method;

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
    
    public WorkflowStateConstant apply(Activity activity, Object... args)
    {
        try
        {
            return (WorkflowStateConstant)method.invoke(activity, args);
        } 
        catch (Exception e)
        {
        	System.err.println("Action on transition in activity " + activity + " args:" + args);
        	for (Object x : args) System.err.println(x);
        	e.printStackTrace(System.err);
            throw new RuntimeException(e);
        } 
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