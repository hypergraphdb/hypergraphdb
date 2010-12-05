package org.hypergraphdb.event;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGReplaceRefusedException;

/**
 * <p>
 * A <code>HGAtomReplaceRequestEvent</code> is triggered when an attempt is made to replace
 * the value of an existing atom within the HyperGraph, but before the replacement process proceeds. 
 * This event gives a chance to 
 * an application to cancel the replacement of the atom. A listener to this event may return
 * the <code>HGListener.Result.cancel</code> code which will prevent the change from happening.
 * On the other hand, if an application needs to perform some action as a result to an already 
 * completed replacement, the application should then listen to the <code>HGAtomReplacedEvent</code>. 
 * </p>
 * 
 * <p>
 * A listener may also throw a {@link HGReplaceRefusedException} which will fail the current 
 * transaction and must be caught at the application level.  
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class HGAtomReplaceRequestEvent extends HGAtomEvent 
{
    private HGHandle type;
    private Object newValue;
    
    /**
     * <p>Construct a new <code>HGAtomReplaceRequestEvent</code> for the
     * given atom.</p>
     * 
     * @param handle The <code>HGHandle</code> of the atom.
     */
    public HGAtomReplaceRequestEvent(HGHandle handle, HGHandle type, Object newValue)
    {
        super(handle);
        this.type = type;
        this.newValue = newValue;
    }

    public Object getNewValue()
    {
        return newValue;
    }

    public void setNewValue(Object newValue)
    {
        this.newValue = newValue;
    }

    public HGHandle getType()
    {
        return type;
    }

    public void setType(HGHandle type)
    {
        this.type = type;
    }    
}