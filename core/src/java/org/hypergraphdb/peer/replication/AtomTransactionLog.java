package org.hypergraphdb.peer.replication;

/**
 * <p>
 * Track atom changes during a transaction. Changes that cancel each other out,
 * like adding an atom and then removing it will be consolidated so that only
 * the last version of an atom will be passed over to the persistent log.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class AtomTransactionLog
{
    
}