package org.hypergraphdb.peer.workflow;

import static org.hypergraphdb.peer.HGDBOntology.SLOT_GET_OBJECT;
import static org.hypergraphdb.peer.HGDBOntology.SLOT_QUERY;
import static org.hypergraphdb.peer.Structs.combine;
import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.struct;
import static org.hypergraphdb.peer.Structs.object;
import static org.hypergraphdb.peer.Structs.list;
import static org.hypergraphdb.peer.Messages.*;

import java.util.ArrayList;
import java.util.UUID;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.Messages;
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
    public void handleMessage(Message msg)
    {
        boolean getObject = (Boolean) getPart(msg,
                                              Messages.CONTENT,
                                              SLOT_GET_OBJECT);
        Object query = getPart(msg, Messages.CONTENT, SLOT_QUERY);
        Object reply = getReply(msg);

        if (query instanceof HGHandle)
        {
            StorageGraph subgraph = getThisPeer().getSubgraph((HGHandle) query);

            combine(reply, struct(Messages.CONTENT, list(object(subgraph))));
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
                    resultingContent.add(object(getThisPeer().getSubgraph(handle)));
                }
                else
                {
                    resultingContent.add(getThisPeer().getGraph()
                            .getPersistentHandle(handle));
                }
            }

            combine(reply, struct(Messages.CONTENT, resultingContent));
        }
        else
        {
            combine(reply, struct(Messages.CONTENT, null));
        }
        getPeerInterface().send(getSender(msg), reply);
    }
}