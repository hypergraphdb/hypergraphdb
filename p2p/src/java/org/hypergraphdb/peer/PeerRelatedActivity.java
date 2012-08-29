/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

import java.util.concurrent.Callable;

import mjson.Json;

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
	protected Json msg;
	
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

	public Json getMessage()
	{
		return msg;
	}

	public void setMessage(Json msg)
	{
		this.msg = msg;
	}	
}
