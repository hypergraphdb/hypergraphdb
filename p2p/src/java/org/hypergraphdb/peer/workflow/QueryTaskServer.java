/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;


import static org.hypergraphdb.peer.HGDBOntology.SLOT_GET_OBJECT;

import static org.hypergraphdb.peer.HGDBOntology.SLOT_QUERY;
import static org.hypergraphdb.peer.Messages.*;

import java.util.ArrayList;
import java.util.UUID;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.storage.StorageGraph;

public class QueryTaskServer extends Activity
{
    public QueryTaskServer(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public void initiate()
    {
    }

    @Override
    public void handleMessage(Json msg)
    {
        boolean getObject = msg.at(Messages.CONTENT).at(SLOT_GET_OBJECT).asBoolean();
        Object query = Messages.fromJson(msg.at(Messages.CONTENT).at(SLOT_QUERY));
        Json reply = getReply(msg);

        if (query instanceof HGHandle)
        {
            StorageGraph subgraph = getThisPeer().getSubgraph((HGHandle) query);
            reply.set(Messages.CONTENT, subgraph);
        }
        else if (query instanceof HGQueryCondition)
        {
            HGSearchResult<HGHandle> results = getThisPeer().getGraph()
                    .find((HGQueryCondition) query);

            ArrayList<Object> resultingContent = new ArrayList<Object>();
            while (results.hasNext())
            {
                HGHandle handle = results.next();

                if (getObject)
                {
                    resultingContent.add(SubgraphManager.encodeSubgraph(getThisPeer().getSubgraph(handle)));
                }
                else
                {
                    resultingContent.add(getThisPeer().getGraph()
                            .getPersistentHandle(handle));
                }
            }
            reply.set(Messages.CONTENT, resultingContent);
        }
        else
        {
            reply.set(Messages.CONTENT, null);
        }
        getPeerInterface().send(getSender(msg), reply);
    }
}