package org.hypergraphdb.peer;

/**
 * @author Cipri Costa
 * 
 * The interface is used to implement partitioning ... but it is just a first version. 
 * Will probably be removed soon.
 */
public interface PeerPolicy
{
	boolean shouldStore(Object atom);
}
