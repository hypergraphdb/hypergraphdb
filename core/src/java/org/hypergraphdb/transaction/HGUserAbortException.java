package org.hypergraphdb.transaction;

/**
 * 
 * <p>
 * This exception is to be thrown by application code to force aborting
 * the current transaction within a transaction closure.
 * </p>
 *
 * <p>
 * When using the {@link HGTransactionManager.transact} method, one supplies
 * a transaction closure, a {@link Callable} instance that perform the transaction
 * work. However, the <code>transact</code> will always attempt to commit the
 * transaction upon return from the user supplied <code>Callable</code>. If the
 * application decides to abort the transaction <strong>without retrying it</strong>, it should throw 
 * this exception instead of calling {@link HGTransactionManager.abort}. The alternative
 * would be for the application to implement its own transaction retry loop, which is
 * longer and error prone.   
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGUserAbortException extends RuntimeException
{
    private static final long serialVersionUID = 6432485442575625321L;
}