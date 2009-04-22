package org.hypergraphdb.peer;

import java.util.concurrent.Callable;

/**
 * <p>
 * A callable object that executes a task against a given peer with a 
 * <code>boolean</code> result indicating success or failure.
 * </p>
 * 
 * @author Cipri Costa 
 */
public abstract class PeerRelatedActivity implements Callable<Boolean>
{
	protected Object target;
	protected Message msg;
	
	public PeerRelatedActivity()
	{
	}
	
	public Object getTarget()
	{
		return target;
	}

	public void setTarget(Object target)
	{
		this.target = target;
	}

	public Message getMessage()
	{
		return msg;
	}

	public void setMessage(Message msg)
	{
		this.msg = msg;
	}	
}