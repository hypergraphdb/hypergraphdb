package org.hypergraphdb;

import org.hypergraphdb.event.HGAtomReplaceRequestEvent;

/**
 * <p>
 * This exception is thrown when an attempt to replace the value of an existing atom 
 * failed. Generally, this exception is thrown as a response to the {@link HGAtomReplaceRequestEvent}
 * and can be caught by an application that expects it to occur. Listener of the
 * {@link HGAtomReplaceRequestEvent} can be defined to implement various database integrity constraints or
 * even application-level constraints that are otherwise cumbersome to enforce. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGReplaceRefusedException extends HGException
{
    private static final long serialVersionUID = -1938774099871282134L;
    
    private HGHandle atom;
    private Object reason;
    
    /**
     * <p>
     * Default constructor. Normally used only for serialization/de-serialization purposes.
     * Call <code>setAtom</code> and <code>setReason</code> subsequently.
     * </p>
     */
    public HGReplaceRefusedException()
    {
        super("Replace of hypergraph atom failed");        
    }

    /**
     * <p>
     * </p>
     * @param atom
     * @param reason
     */
    public HGReplaceRefusedException(HGHandle atom, Object reason)
    {
        super("Replace of hypergraph atom failed");
        this.atom = atom;
        this.reason = reason;
    }

    /**
     * Return the proposed atom whose addition failed.
     */
    public HGHandle getAtom()
    {
        return atom;
    }

    /**
     * Set the proposed atom whose addition failed.
     * @param atom
     */
    public void setAtom(HGHandle atom)
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