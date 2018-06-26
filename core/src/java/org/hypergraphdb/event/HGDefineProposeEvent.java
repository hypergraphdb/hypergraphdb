package org.hypergraphdb.event;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * Event triggered when a new atom is about to be defined to the graph. Listeners
 * may perform some other operation before the atom is defined or may throw an
 * exception to prevent the atom from being added. Returning <code>HGListener.Result.cancel</code>
 * from the listener will also stop the atom addition, but without communicating further information
 * upstream to the application.
 * </p>
 * 
 * <p>
 * This class inherits from {@link HGAtomProposeEvent} because defining a special case 
 * of adding an atom and listeners can just work with the {@link HGAtomProposeEvent}. Note
 * however that you must register to {@link HGAtomProposeEvent} and <code>HGDefineProposeEvent</code>
 * separately.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGDefineProposeEvent extends HGAtomProposeEvent
{
	private HGHandle atomHandle;
	
	public HGDefineProposeEvent(HGHandle atomHandle, Object atom, HGHandle type, int flags)
	{
		super(atom, type, flags);
		this.atomHandle = atomHandle;
	}
	
	public HGHandle getAtomHandle()
	{
		return this.atomHandle;
	}
}
