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
