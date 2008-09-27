package org.hypergraphdb.peer;
import static org.hypergraphdb.peer.HGDBOntology.*;
import static org.hypergraphdb.peer.Structs.*;


/**
 * @author Cipri Costa
 *
 * Class with static methods to help manipulate messages.
 */
public class Messages
{
	public static Object createMessage(Object performative, Object action, Object taskId)
	{
		return struct(PERFORMATIVE, performative, ACTION, action, SEND_TASK_ID, taskId);
	}
	
	public static Object getReply(Object msg)
	{
		return struct(ACTION, getPart(msg, ACTION),
				CONVERSATION_ID, getPart(msg, CONVERSATION_ID),
				RECEIVED_TASK_ID, getPart(msg, SEND_TASK_ID),
				SEND_TASK_ID, getPart(msg, SEND_TASK_ID));
	}
}
