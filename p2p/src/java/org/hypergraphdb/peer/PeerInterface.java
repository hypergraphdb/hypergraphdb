/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

import java.util.concurrent.Future;
import mjson.Json;

/**
 * <p>
 * This interface define the low-level communication layer for the HyperGraphDB P2P 
 * framework. Implementations can use any transport and any presence model they wish.
 * </p>
 * <p>
 * The interface has some factory methods that allow implementers to decide how to create
 * and allocate objects. 
 * </p>
 * 
 * @author Cipri Costa, Borislav Iordanov
 */
public interface PeerInterface
{
    /**
     * <p>
     * The <code>MessageHandler</code> is responsible for processing messages coming through
     * the <code>PeerInterface</code>. The <code>PeerInterface</code> merely handles transport
     * duties, but it delegates the logic for message handling elsewhere. In the HyperGraphDB P2P 
     * framework, that elsewhere is the {@link ActivityManager}. 
     * </p>
     * @param message
     */
    void setMessageHandler(MessageHandler messageHandler);
    
	/**
	 * Because implementors can be of any type, the configuration is an Object, no constraints 
	 * to impose here as there is no common set of configuration properties.
	 * 
	 * @param configuration A JSON-like structure holding the network-level configuration parameters.
	 * @throws RuntimeException This method may throw an unchecked exception if the configuration
	 * is not correct.
	 */
	void configure(Json configuration);
	
	/**
	 * <p>
	 * Establish a connection with other peers and make one's presence in the network 
	 * known.
	 * </p>
	 */
	void start();

	/**
	 * <p>Return <code>true</code> if we are currently connected to the network
	 * and <code>false</code> otherwise. Because presence is negotiated asynchronously,
	 * a connection doesn't imply that all peers are already known.</p>
	 */
	boolean isConnected();
	
	/**
	 * <p>
	 * Disconnect from the P2P network. No more messages are going to be received or sent.
	 * </p>
	 */
    void stop();
    
    /**
     * <p>
     * Return the <code>HyperGraphPeer</code> to which this <code>PeerInterface</code>
     * is bound.
     * </p>
     */
    HyperGraphPeer getThisPeer();
    
    /**
     * <p>
     * Internally used to initialize the <code>PeerInterface</code>, don't call in 
     * application code. Implementation should maintain a <code>HyperGraphPeer</code>
     * member variable and return it in the <code>getThisPeer</code> method.
     * </p> 
     */
    void setThisPeer(HyperGraphPeer thisPeer);
    
	//factory methods to obtain activities that are specific to the peer implementation
	//TODO redesign
	PeerFilter newFilterActivity(PeerFilterEvaluator evaluator);
	PeerRelatedActivityFactory newSendActivityFactory();
	
	/**
	 * <p>
	 * Broadcast a message to all members of this peer's group.
	 * </p>
	 * 
	 * @param msg
	 */
	void broadcast(Json msg);

	/**
	 * <p>
	 * Send a message to a specific peer as identified by the
	 * <code>networkTarget</code> parameter.
	 * </p>
	 * 
	 * @param networkTarget
	 * @param msg
	 * @return
	 */
	Future<Boolean> send(Object networkTarget, Json msg);
	
    void addPeerPresenceListener(NetworkPeerPresenceListener listener);
    void removePeerPresenceListener(NetworkPeerPresenceListener listener);
	
}