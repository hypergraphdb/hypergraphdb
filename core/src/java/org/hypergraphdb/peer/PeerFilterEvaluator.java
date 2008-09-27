package org.hypergraphdb.peer;

/**
 * @author ciprian.costa
 * Implementors can specify the logic for filtering peers
 */
public interface PeerFilterEvaluator
{
	boolean shouldSend(Object target);
}
