/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * <p>
 * The FIPA standard communicative acts. 
 * </p>
 * 
 * @author Cipri Costa and Borislav Iordanov
 */
public class Performative
{
    private static Map<String, PerformativeConstant> constantPool = 
        new IdentityHashMap<String, PerformativeConstant>();
    
    private String name;
    
    Performative(String name)
    {
        this.name = name;
    }
    
    public static synchronized PerformativeConstant makeConstant(String name)
    {
        String interned = name.intern();
        PerformativeConstant result = constantPool.get(interned);
        if (result == null)
        {
            result = new PerformativeConstant(interned);
            constantPool.put(interned, result);            
        }
        return result;
    }
    
    public synchronized static PerformativeConstant toConstant(String name)
    {
        PerformativeConstant c = constantPool.get(name.intern());
        if (c == null)
            throw new RuntimeException("Unknown performative constant: " + name);
        return c;
    }
    
    public int hashCode() 
    { 
        return name.hashCode();
    }
    
    public boolean equals(Object x) 
    {
        if (x == this) 
            return true;
        else if (! (x instanceof Performative)) 
            return false;
        else 
            return ((Performative)x).name == name;
    }
    
    public String toString()
    {
        return name;
    }
    
    /**
     * The action of accepting a previously submitted proposal to perform an action.
     */
	public static final PerformativeConstant AcceptProposal = makeConstant("AcceptProposal");
	
	/**
	 * The action of agreeing to perform some action, possibly in the future.
	 */
	public static final PerformativeConstant Agree = makeConstant("Agree");
    /**
     * The action of one agent informing another agent that the first agent no 
     * longer has the intention that the second agent performs some action.
     */
	public static final PerformativeConstant Cancel = makeConstant("Cancel");
	/**
	 * The action of calling for proposals to perform a given action.
	 */
	public static final PerformativeConstant CallForProposal = makeConstant("CallForProposal");
    /**
     * The sender informs the receiver that a given proposition is true, 
     * where the receiver is known to be uncertain about the proposition.
     */
	public static final PerformativeConstant Confirm = makeConstant("Confirm");
    /**
     * The sender informs the receiver that a given proposition is false, where 
     * the receiver is known to believe, or believe it likely that, the proposition is true.
     */	
	public static final PerformativeConstant Disconfirm = makeConstant("Disconfirm");
    /**
     * The action of telling another agent that an action was attempted but the attempt failed.
     */
	public static final PerformativeConstant Failure = makeConstant("Failure");
    /**
     * The sender informs the receiver that a given proposition is true.
     */
	public static final PerformativeConstant Inform = makeConstant("Inform"); 
    /**
     * A macro action for the agent of the action to inform the recipient 
     * whether or not a proposition is true.
     */
	public static final PerformativeConstant InformIf = makeConstant("InformIf");
    /**
     * A macro action for sender to inform the receiver the object which 
     * corresponds to a descriptor, for example, a name.
     */
	public static final PerformativeConstant InformRef = makeConstant("InformRef");
    /**
     * The sender of the act (for example, i) informs the receiver (for example, 
     * j) that it perceived that j  performed some action, but that i did not 
     * understand what j  just did. A particular common case is that i tells j 
     * that i  did not understand the message that j has just sent to i.
     */
	public static final PerformativeConstant NotUnderstood = makeConstant("NotUnderstood");
    /**
     * The sender intends that the receiver treat the embedded message as sent 
     * directly to the receiver, and wants the receiver to identify the agents 
     * denoted by the given descriptor and send the received propagate message 
     * to them. 
     */
	public static final PerformativeConstant Propagate = makeConstant("Propagate");
    /**
     * The action of submitting a proposal to perform a certain action, given 
     * certain preconditions.
     */
	public static final PerformativeConstant Propose = makeConstant("Propose");
    /**
     * The sender wants the receiver to select target agents denoted by a given 
     * description and to send an embedded message to them. 
     */
	public static final PerformativeConstant Proxy = makeConstant("Proxy");
    /**
     * The action of asking another agent whether or not a given proposition 
     * is true.
     */
	public static final PerformativeConstant QueryIf = makeConstant("QueryIf");
    /**
     * The action of asking another agent for the object referred to by a 
     * referential expression.
     */
	public static final PerformativeConstant QueryRef = makeConstant("QueryRef");
    /**
     * The action of refusing to perform a given action, and explaining 
     * the reason for the refusal.
     */
	public static final PerformativeConstant Refuse = makeConstant("Refuse");
    /**
     * The action of rejecting a proposal to perform some action during 
     * a negotiation.
     */
	public static final PerformativeConstant RejectProposal = makeConstant("RejectProposal");	  
    /**
     * The sender requests the receiver to perform some action. One 
     * important class of uses of the request act is to request the receiver 
     * to perform another communicative act.
     */
	public static final PerformativeConstant Request = makeConstant("Request");
    /**
     * The sender wants the receiver to perform some action when some 
     * given proposition becomes true.
     */
	public static final PerformativeConstant RequestWhen = makeConstant("RequestWhen");
    /**
     * The sender wants the receiver to perform some action as soon 
     * as some proposition becomes true and thereafter each time 
     * the proposition becomes true again.
     */
	public static final PerformativeConstant RequestWhenever = makeConstant("RequestWhenever");
	/**
	 * The act of requesting a persistent intention to notify the 
	 * sender of the value of a reference, and to notify again whenever 
	 * the object identified by the reference changes.
	 */
	public static final PerformativeConstant Subscribe = makeConstant("Subscribe");
}
