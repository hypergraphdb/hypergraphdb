/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;
import static mjson.Json.*;
import java.util.UUID;
import mjson.Json;

import org.hypergraphdb.peer.serializer.HGPeerJsonFactory;
import org.hypergraphdb.peer.workflow.Activity;


/**
 * @author Cipri Costa
 *
 * Class with static methods to help manipulate messages.
 */
public class Messages
{
	public static <T> T fromJson(Json j)
	{
		return HGPeerJsonFactory.getInstance().value(j);
	}

	public static <T> T content(Json j)
	{
		return HGPeerJsonFactory.getInstance().value(j.at(CONTENT));
	}
	
    public static Json createMessage(Performative performative, Activity activity)
    {
        return createMessage(performative, activity.getType(), activity.getId());
    }
    
	public static Json createMessage(Performative performative, String type, UUID activityId)
	{
		return object(PERFORMATIVE, performative.toString(), 
		              ACTIVITY_TYPE, type, 
		              CONVERSATION_ID, activityId);
	}

    public static Json getReply(Json msg, Performative performative, Object content)
    {
        return getReply(msg, performative).set(CONTENT, content);
    }
	
	public static Json getReply(Json msg, Performative performative)
	{
	    return getReply(msg).set(PERFORMATIVE, performative.toString());
	}
	
	public static Json makeReply(Activity activity, Performative performative, String replyWith)
	{
		Json s = object(ACTIVITY_TYPE, activity.getType(),
                        CONVERSATION_ID, activity.getId(),
                        PERFORMATIVE, performative.toString());        
        if (replyWith != null)
            return s.set(IN_REPLY_TO, replyWith);
        else
            return s;	    
	}
	
	public static Json getReply(Json msg)
	{
		Json s = object(ACTIVITY_TYPE, msg.at(ACTIVITY_TYPE),
		                CONVERSATION_ID, msg.at(CONVERSATION_ID));
		if (msg.has(PARENT_SCOPE))
			s.set(PARENT_SCOPE, msg.at(PARENT_SCOPE))
		     .set(PARENT_TYPE, msg.at(PARENT_TYPE));
		return msg.has(REPLY_WITH) ? s.set(IN_REPLY_TO, msg.at(REPLY_WITH)) : s;
	}
	
	/**
	 * <p>
	 * Return the network identity of the sender of a given message.
	 * </p>
	 * @param msg
	 * @return
	 */
	public static Object getSender(Json msg)
	{
	    return Messages.fromJson(msg.at(Messages.REPLY_TO));	  
	}

    public static final String PERFORMATIVE = "performative";
    public static final String IN_REPLY_TO = "in-reply-to";
    public static final String REPLY_WITH = "reply-with";
    public static final String CONTENT = "content";
    public static final String LANGUAGE = "language";
    public static final String OPERATION = "operation";
    public static final String REPLY_TO = "reply-to";
    public static final String CONVERSATION_ID = "conversation-id";
    public static final String PARENT_SCOPE = "x-parent-scope";
    public static final String PARENT_TYPE = "x-parent-type";
    public static final String ACTIVITY_TYPE = "x-activity-type";
    public static final String WHY_NOT_UNDERSTOOD = "x-why-not-understood";
}