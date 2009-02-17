package org.hypergraphdb.peer.workflow;

import static org.hypergraphdb.peer.HGDBOntology.CONTENT;
import static org.hypergraphdb.peer.HGDBOntology.SEND_TASK_ID;
import static org.hypergraphdb.peer.HGDBOntology.SLOT_GET_OBJECT;
import static org.hypergraphdb.peer.HGDBOntology.SLOT_QUERY;
import static org.hypergraphdb.peer.HGDBOntology.REPLY_TO;

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
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.Subgraph;
import org.hypergraphdb.query.HGQueryCondition;

public class QueryTaskServer extends TaskActivity<QueryTaskServer.State>
{
	protected enum State {Started, Done}
	
	private Object msg;
	
	public QueryTaskServer(HyperGraphPeer thisPeer, Object msg)
	{
		super(thisPeer, (UUID)getPart(msg, SEND_TASK_ID), State.Started, State.Done);
		this.msg = msg;
	}

	@Override
	protected void startTask()
	{
		boolean getObject = (Boolean)getPart(msg, CONTENT, SLOT_GET_OBJECT);
		Object query = getPart(msg, CONTENT, SLOT_QUERY);
		Object reply = getReply(msg);
		
		if (query instanceof HGHandle)
		{
			Subgraph subgraph = getThisPeer().getSubgraph((HGHandle)query);
			
			combine(reply, struct(CONTENT, list(object(subgraph))));
		}else if (query instanceof HGQueryCondition){
			HGSearchResult<HGHandle> results = getThisPeer().getGraph().find((HGQueryCondition)query);

			ArrayList<Object> resultingContent = new ArrayList<Object>();
			while(results.hasNext())
			{
				HGHandle handle = results.next();
				
				if (getObject)
				{
					resultingContent.add(object(getThisPeer().getSubgraph(handle)));
				}else{
					resultingContent.add(getThisPeer().getGraph().getPersistentHandle(handle));
				}
			}
			
			combine(reply, struct(CONTENT, resultingContent));
		}else{
			combine(reply, struct(CONTENT, null));
		}

		PeerRelatedActivityFactory activityFactory = getPeerInterface().newSendActivityFactory();

		PeerRelatedActivity activity = (PeerRelatedActivity)activityFactory.createActivity();
		activity.setTarget(getPart(msg, REPLY_TO));
		activity.setMessage(reply);
		
		getPeerInterface().execute(activity);

		
	}

	
	public static class QueryTaskFactory implements TaskFactory
	{
		public QueryTaskFactory()
		{
		}
		public TaskActivity<?> newTask(HyperGraphPeer peer, Object msg)
		{
			return new QueryTaskServer(peer, msg);
		}
		
	}

}
