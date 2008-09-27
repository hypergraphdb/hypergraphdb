package org.hypergraphdb.peer;

/**
 * @author Cipri Costa
 * A runnable object that executes a task against a given peer 
 */
public abstract class PeerRelatedActivity implements Runnable
{
	protected Object target;
	protected Object msg;
	
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

	public Object getMessage()
	{
		return msg;
	}

	public void setMessage(Object msg)
	{
		this.msg = msg;
	}
	
}
