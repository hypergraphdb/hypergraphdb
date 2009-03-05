package org.hypergraphdb.peer;

/**
 * <p>
 * Represents an exception that occurred at a remote peer and caused it to fail
 * to participate in an activity.
 * </p>
 * 
 * <p>
 * The message returned by <code>getMessage</code> will normally be the full
 * stack-trace of the exception at the peer and it's only useful for reporting 
 * purposes.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class ExceptionAtPeer extends RuntimeException
{
    private static final long serialVersionUID = 1L;
    
    private HGPeerIdentity peer;
    
    public ExceptionAtPeer(HGPeerIdentity peer, String text)
    {
        super(text);
        this.peer = peer;
    }
    
    /**
     * <p>Return the identity of the peer on which the exception occurred.</p>
     */
    public HGPeerIdentity getPeer()
    {
        return peer;
    }
}