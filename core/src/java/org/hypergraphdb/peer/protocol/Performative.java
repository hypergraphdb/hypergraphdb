package org.hypergraphdb.peer.protocol;

/**
 * <p>
 * The FIPA standard communicative acts. 
 * </p>
 * 
 * @author Cipri Costa and Borislav Iordanov
 */
public enum Performative
{
	Accept,
	AcceptProposal,
	Agree,
	CallForProposal,
	Confirm,
	Disconfirm,
	Failure, 
	Inform, 
	InformIf,
	InformRef,
	NotUnderstood,
	Propagate,
	Propose,
	Proxy,
	QueryIf,
	QueryRef,
	Refuse,	
	RejectProposal,	  
	Request,
	RequestWhen,
	RequestWhenever,
	Subscribe
}