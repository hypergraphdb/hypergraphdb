/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.replication.Replication;
import org.hypergraphdb.query.HGAtomPredicate;

/**
 * @author ciprian.costa
 * Evaluates if a peer passes the filtering process based on the interests it 
 * announced previously.
 */
public class InterestEvaluator implements PeerFilterEvaluator
{
	private PeerInterface peerInterface;
	private HyperGraph hg;
	private HGHandle handle;
	
	public InterestEvaluator(PeerInterface peerInterface, HyperGraph hg)
	{
		this.peerInterface = peerInterface;
		this.hg = hg;
	}
	
	public boolean shouldSend(Object target)
	{
		System.out.println("InterestsPeerFilterEvaluator: evaluating " + handle + " for " + target);
		
		HGPeerIdentity id = peerInterface.getThisPeer().getIdentity(target);
		if (id == null)
		    return false;		
		HGAtomPredicate pred = Replication.get(peerInterface.getThisPeer()).getOthersInterests().get(id);
		return (pred != null) && pred.satisfies(hg, handle);
	}

	public PeerInterface getPeerInterface()
	{
		return peerInterface;
	}

	public void setPeerInterface(PeerInterface peerInterface)
	{
		this.peerInterface = peerInterface;
	}

	public HyperGraph getHg()
	{
		return hg;
	}

	public void setHg(HyperGraph hg)
	{
		this.hg = hg;
	}

	public HGHandle getHandle()
	{
		return handle;
	}

	public void setHandle(HGHandle handle)
	{
		this.handle = handle;
	}
	
}
