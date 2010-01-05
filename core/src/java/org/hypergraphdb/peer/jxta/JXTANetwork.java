/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.jxta;

import java.util.Set;

import net.jxta.document.Advertisement;
import net.jxta.peergroup.PeerGroup;
import net.jxta.pipe.PipeID;

//import org.hypergraphdb.peer.PeerNetwork;

public interface JXTANetwork // extends PeerNetwork
{
	PeerGroup getPeerGroup();
	
	void publishAdv(Advertisement adv);
	void addOwnPipe(PipeID pipeId);
	Set<Advertisement> getAdvertisements();
	Advertisement getPipeAdv();

	
}
