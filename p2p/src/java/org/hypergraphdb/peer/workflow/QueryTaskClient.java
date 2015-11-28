/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;


import static org.hypergraphdb.peer.HGDBOntology.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.storage.StorageGraph;

public class QueryTaskClient extends Activity
{
    private AtomicInteger count = new AtomicInteger(1);

    private Iterator<Object> targets = null;

    private HGHandle handle;
    private HGQueryCondition cond;
    private boolean getObject;
    private HyperGraph tempGraph;

    private ArrayList<Object> result;

    public QueryTaskClient(HyperGraphPeer thisPeer, HyperGraph tempGraph)
    {
        super(thisPeer);
        this.tempGraph = tempGraph;
    }

    public QueryTaskClient(HyperGraphPeer thisPeer, HyperGraph tempGraph,
                           HGQueryCondition cond, boolean getObject)
    {
        super(thisPeer);
        this.handle = null;
        this.cond = cond;
        this.getObject = getObject;
        this.tempGraph = tempGraph;
    }

    public QueryTaskClient(HyperGraphPeer thisPeer, HyperGraph tempGraph,
                           Iterator<Object> targets, HGQueryCondition cond,
                           boolean getObject)
    {
        super(thisPeer);

        this.targets = targets;
        this.handle = null;
        this.cond = cond;
        this.getObject = getObject;
        this.tempGraph = tempGraph;
    }

    public QueryTaskClient(HyperGraphPeer thisPeer, HyperGraph tempGraph,
                           HGHandle handle)
    {
        super(thisPeer);

        this.handle = handle;
        this.getObject = true;
        this.tempGraph = tempGraph;
    }

    public QueryTaskClient(HyperGraphPeer thisPeer, HyperGraph tempGraph,
                           Iterator<Object> targets, HGHandle handle)
    {
        super(thisPeer);

        this.targets = targets;
        this.handle = handle;
        this.getObject = true;
        this.tempGraph = tempGraph;
    }

    public void initiate()
    {
        PeerRelatedActivityFactory activityFactory = getPeerInterface().newSendActivityFactory();
        while (targets.hasNext())
//		for (HGPeerIdentity peer : getThisPeer().getConnectedPeers())
		{
			Object target = targets.next(); //getThisPeer().getNetworkTarget(peer);
            sendMessage(activityFactory, target);
        }

        if (count.decrementAndGet() == 0)
        {
            getState().assign(WorkflowState.Completed);
        }
    }

    private void sendMessage(PeerRelatedActivityFactory activityFactory,
                             Object target)
    {
        count.incrementAndGet();

        Json msg = Messages.createMessage(Performative.Request, QUERY, getId());
        msg.set(Messages.CONTENT, Json.object(SLOT_QUERY,
                                            (handle == null) ? cond : handle,
                                            SLOT_GET_OBJECT, getObject));

        PeerRelatedActivity activity = (PeerRelatedActivity) activityFactory.createActivity();
        activity.setTarget(target);
        activity.setMessage(msg);

        getThisPeer().getExecutorService().submit(activity); // TODO: what about the result??
    }

    public void handleMessage(Json msg)
    {
        // get result
        Json reply = msg.at(Messages.CONTENT);
        result = new ArrayList<Object>();

        for (Json j : reply.asJsonList())
        {
        	Object elem = Messages.fromJson(j); 
            if (elem instanceof StorageGraph)
            {
                result.add(SubgraphManager.get((StorageGraph) elem, tempGraph));
            }
            else
            {
                result.add(elem);
            }
        }

        if (count.decrementAndGet() == 0)
        {
            getState().assign(WorkflowState.Completed);
        }
    }

    public ArrayList<Object> getResult()
    {
        return result;
    }

    public void setResult(ArrayList<Object> result)
    {
        this.result = result;
    }
}