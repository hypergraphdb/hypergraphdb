package org.hypergraphdb.peer.protocol;

/**
 * @author Cipri Costa
 * This will be refactored to become an open set.
 */
public enum Performative
{
	CallForProposal, 
	Proposal, 
	Inform, 
	InformRef, 
	Accept, 
	RejectProposal, 
	Confirm, 
	Disconfirm, 
	Request,
	NotUnderstood
}