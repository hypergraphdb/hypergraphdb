/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.log;

import org.hypergraphdb.peer.HGPeerIdentity;


/**
 * @author ciprian.costa
 * Simple bean that stores all the required information about a peer.
 */
public class Peer
{
	private HGPeerIdentity peerId;
	private Timestamp timestamp = new Timestamp();
	private Timestamp lastConfirmedTimestamp =  new Timestamp();
	private Timestamp lastFrom = new Timestamp();
	
	public Peer()
	{
		
	}
	public Peer(HGPeerIdentity peerId)
	{
		this.peerId = peerId;
	}

	public HGPeerIdentity getPeerId()
	{
		return peerId;
	}

	public void setPeerId(HGPeerIdentity peerId)
	{
		this.peerId = peerId;
	}
	public Timestamp getTimestamp()
	{
		return timestamp;
	}
	public void setTimestamp(Timestamp timestamp)
	{
		this.timestamp = timestamp;
	}
	public Timestamp getLastConfirmedTimestamp()
	{
		return lastConfirmedTimestamp;
	}
	public void setLastConfirmedTimestamp(Timestamp lastConfirmedTimestamp)
	{
		this.lastConfirmedTimestamp = lastConfirmedTimestamp;
	}
	public Timestamp getLastFrom()
	{
		return lastFrom;
	}
	public void setLastFrom(Timestamp lastFrom)
	{
		this.lastFrom = lastFrom;
	}
	
	public String toString()
	{
		return "Peer: " + peerId + "; timestamp: " + timestamp + "; last confirmed: " + lastConfirmedTimestamp + "; last from: " + lastFrom;
	}
	
}
