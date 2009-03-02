package org.hypergraphdb.peer;
import static org.hypergraphdb.peer.HGDBOntology.*;
import static org.hypergraphdb.peer.Structs.*;

import java.util.Map;
import java.util.UUID;

import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.peer.workflow.Activity;


/**
 * @author Cipri Costa
 *
 * Class with static methods to help manipulate messages.
 */
public class Messages
{
    public static Message createMessage(Performative performative, Activity activity)
    {
        return createMessage(performative, activity.getType(), activity.getId());
    }
    
	public static Message createMessage(Performative performative, String type, UUID activityId)
	{
		return new Message(struct(PERFORMATIVE, performative, 
		                          ACTIVITY_TYPE, type, 
		                          CONVERSATION_ID, activityId));
	}
	
	public static Message getReply(Message msg)
	{
		Map<String, Object> s = struct(ACTIVITY_TYPE, getPart(msg, ACTIVITY_TYPE),
		                               CONVERSATION_ID, getPart(msg, CONVERSATION_ID));
		String replyWith = getPart(msg, REPLY_WITH);
		if (replyWith != null)
		    return new Message(combine(s, struct(IN_REPLY_TO, replyWith)));
		else
		    return new Message(s);
	}
	
	public static Object getSender(Message msg)
	{
	    return getPart(msg, REPLY_TO);	  
	}
}