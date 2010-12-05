package org.hypergraphdb.event;

import org.hypergraphdb.HGException;

/**
 * <p>
 * This exception is thrown when an attempt to add a new atom to a HyperGraph instance
 * failed. Generally, this exception is thrown as a response to the {@link HGAtomProposeEvent}
 * and can be caught by an application that expects it to occur. Listener of the
 * {@HGAtomProposedEvent} can be defined to implement various database integrity constraints or
 * even application-level constraints that are otherwise cumbersome to enforce. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGAtomRefusedException extends HGException
{
    private static final long serialVersionUID = -8950893271752138729L;
    
    private Object atom;
    private Object reason;
    
    /**
     * <p>
     * Default constructor. Normally used only for serialization/de-serialization purposes.
     * Call <code>setAtom</code> and <code>setReason</code> subsequently.
     * </p>
     */
    public HGAtomRefusedException()
    {
        super("Addition of new hypergraph atom failed because.");        
    }

    /**
     * <p>
     * </p>
     * @param atom
     * @param reason
     */
    public HGAtomRefusedException(Object atom, Object reason)
    {
        super("Addition of new hypergraph atom failed because.");
        this.atom = atom;
        this.reason = reason;
    }

    /**
     * Return the proposed atom whose addition failed.
     */
    public Object getAtom()
    {
        return atom;
    }

    /**
     * Set the proposed atom whose addition failed.
     * @param atom
     */
    public void setAtom(Object atom)
    {
        this.atom = atom;
    }

    /**
     * Return the reason of the failure - could be a string or a more complex
     * object interpreted by the application at hand.
     */
    public Object getReason()
    {
        return reason;
    }

    /**
     * Set the reason for the failure (a string or anything else an application
     * can make use of). 
     * @param reason
     */
    public void setReason(Object reason)
    {
        this.reason = reason;
    }    
}