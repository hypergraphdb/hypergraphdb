/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.jxta;

import net.jxta.protocol.PipeAdvertisement;

import org.hypergraphdb.peer.PeerFilterEvaluator;


/**
 * @author ciprian.costa
 * Will filter peers by name. If name is null, will just return all peers 
 */
public class DefaultPeerFilterEvaluator implements PeerFilterEvaluator
{
	private String targetName;
	
	public DefaultPeerFilterEvaluator(String targetName)
	{
		this.targetName = targetName;
	}
	
	public boolean shouldSend(Object target)
	{
		//for the time being ... something very simple
		if (targetName == null) return true;
		else if (target instanceof PipeAdvertisement)
		{
			return targetName.toString().equals(((PipeAdvertisement)target).getName());
		}
		
		return false;
	}

}
