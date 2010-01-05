/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.jxta;

import net.jxta.document.Advertisement;
import net.jxta.peergroup.PeerGroup;
import net.jxta.protocol.PipeAdvertisement;

import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;

/**
 * @author Cipri Costa
 *
 * Simple factory for JXTASendActivities
 */
public class JXTASendActivityFactory implements PeerRelatedActivityFactory
{
	private PeerGroup peerGroup;
	private Advertisement pipeAdv;
	
	public JXTASendActivityFactory(PeerGroup peerGroup, PipeAdvertisement pipeAdv)
	{
		this.peerGroup = peerGroup;
		this.pipeAdv = pipeAdv;
	}
	
	public PeerRelatedActivity createActivity()
	{
		return new JXTASendActivity(peerGroup, pipeAdv);
	}

}
