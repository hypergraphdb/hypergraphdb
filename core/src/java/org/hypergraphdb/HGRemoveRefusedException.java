package org.hypergraphdb;

public class HGRemoveRefusedException extends HGException
{
	private static final long serialVersionUID = -1967820859656670563L;
	
	private HGHandle atom;
    private Object reason;
    
    /**
     * <p>
     * Default constructor. Normally used only for serialization/de-serialization purposes.
     * Call <code>setAtom</code> and <code>setReason</code> subsequently.
     * </p>
     */
    public HGRemoveRefusedException()
    {
        super("Remove of hypergraph atom failed");        
    }

    /**
     * <p>
     * </p>
     * @param atom
     * @param reason
     */
    public HGRemoveRefusedException(HGHandle atom, Object reason)
    {
        super("Remove of hypergraph atom failed");
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