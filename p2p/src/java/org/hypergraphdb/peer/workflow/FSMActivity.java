/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;

import java.util.UUID;



import mjson.Json;

import org.hypergraphdb.peer.ExceptionAtPeer;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import static org.hypergraphdb.peer.Messages.*;

/**
 * <p>
 * An activity implementation based on a finite state machine defined through
 * method annotations.  
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public abstract class FSMActivity extends Activity
{
    /**
     * <p>
     * Called by default by the framework in case a peer send
     * a <code>Failure</code> performative and there's no transition
     * defined for it. 
     * </p>
     * 
     * <p>
     * The default implementation of this method is to fail the whole
     * activity. To change this behavior, you can either define appropriate 
     * transition for the <code>Failure</code> performative or simply
     * override this method in your activity class. 
     * </p>
     * 
     * @param msg The message that is reporting the peer failure.
     */
    protected void onPeerFailure(Json msg)
    {
        HGPeerIdentity id = getThisPeer().getIdentity(getSender(msg));
        this.future.result.exception = new ExceptionAtPeer(id,
                                                           msg.at(CONTENT).asString());
        getState().assign(WorkflowState.Failed);
    }

    /**
     * <p>
     * Called by default by the framework in case a peer send
     * a <code>NotUnderstand</code> performative and there's no transition
     * defined for it. 
     * </p>
     * 
     * <p>
     * The default implementation of this method is to fail the whole
     * activity. To change this behavior, you can either define appropriate 
     * transition for the <code>NotUnderstand</code> performative or simply
     * override this method in your activity class. 
     * </p>
     * 
     * @param msg The message that is reporting the peer failure.
     */    
    protected void onPeerNotUnderstand(Json msg)
    {
        HGPeerIdentity id = getThisPeer().getIdentity(getSender(msg));
        this.future.result.exception = new ExceptionAtPeer(id,
                                                           "Peer did not understand last message:" +
                                                           msg.at(CONTENT).toString() + ", because " +
                                                           msg.at(WHY_NOT_UNDERSTOOD).asString());
        getState().assign(WorkflowState.Failed);        
    }
    
    /**
     * <p>Default constructor - thisPeer, ID etc must be set separately.</p>
     */
    public FSMActivity()
    {    	
    }
    
    public FSMActivity(HyperGraphPeer thisPeer)
    {
        super(thisPeer);
    }
    
    public FSMActivity(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }
    
    /**
     * <p>Empty method - override to implement activity initiation.</p>
     */
    public void initiate() { }
    
    /**
     * <p>Empty method - can't override because message handling for
     * <code>FSMActivity</code> is automated by the framework.</p>
     */
    public final void handleMessage(Json message) { }
}