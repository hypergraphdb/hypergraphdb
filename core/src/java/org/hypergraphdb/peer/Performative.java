package org.hypergraphdb.peer;

/**
 * <p>
 * The FIPA standard communicative acts. 
 * </p>
 * 
 * @author Cipri Costa and Borislav Iordanov
 */
public enum Performative
{
    /**
     * The action of accepting a previously submitted proposal to perform an action.
     */
	AcceptProposal,
	/**
	 * The action of agreeing to perform some action, possibly in the future.
	 */
	Agree,
    /**
     * The action of one agent informing another agent that the first agent no 
     * longer has the intention that the second agent performs some action.
     */
	Cancel,
	/**
	 * The action of calling for proposals to perform a given action.
	 */
	CallForProposal,
    /**
     * The sender informs the receiver that a given proposition is true, 
     * where the receiver is known to be uncertain about the proposition.
     */
	Confirm,
    /**
     * The sender informs the receiver that a given proposition is false, where 
     * the receiver is known to believe, or believe it likely that, the proposition is true.
     */	
	Disconfirm,
    /**
     * The action of telling another agent that an action was attempted but the attempt failed.
     */
	Failure, 
    /**
     * The sender informs the receiver that a given proposition is true.
     */
	Inform, 
    /**
     * A macro action for the agent of the action to inform the recipient 
     * whether or not a proposition is true.
     */
	InformIf,
    /**
     * A macro action for sender to inform the receiver the object which 
     * corresponds to a descriptor, for example, a name.
     */
	InformRef,
    /**
     * The sender of the act (for example, i) informs the receiver (for example, 
     * j) that it perceived that j  performed some action, but that i did not 
     * understand what j  just did. A particular common case is that i tells j 
     * that i  did not understand the message that j has just sent to i.
     */
	NotUnderstood,
    /**
     * The sender intends that the receiver treat the embedded message as sent 
     * directly to the receiver, and wants the receiver to identify the agents 
     * denoted by the given descriptor and send the received propagate message 
     * to them. 
     */
	Propagate,
    /**
     * The action of submitting a proposal to perform a certain action, given 
     * certain preconditions.
     */
	Propose,
    /**
     * The sender wants the receiver to select target agents denoted by a given 
     * description and to send an embedded message to them. 
     */
	Proxy,
    /**
     * The action of asking another agent whether or not a given proposition 
     * is true.
     */
	QueryIf,
    /**
     * The action of asking another agent for the object referred to by a 
     * referential expression.
     */
	QueryRef,
    /**
     * The action of refusing to perform a given action, and explaining 
     * the reason for the refusal.
     */
	Refuse,	
    /**
     * The action of rejecting a proposal to perform some action during 
     * a negotiation.
     */
	RejectProposal,	  
    /**
     * The sender requests the receiver to perform some action. One 
     * important class of uses of the request act is to request the receiver 
     * to perform another communicative act.
     */
	Request,
    /**
     * The sender wants the receiver to perform some action when some 
     * given proposition becomes true.
     */
	RequestWhen,
    /**
     * The sender wants the receiver to perform some action as soon 
     * as some proposition becomes true and thereafter each time 
     * the proposition becomes true again.
     */
	RequestWhenever,
	/**
	 * The act of requesting a persistent intention to notify the 
	 * sender of the value of a reference, and to notify again whenever 
	 * the object identified by the reference changes.
	 */
	Subscribe
}