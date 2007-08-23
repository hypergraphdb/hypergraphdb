package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;

/**
 * 
 * <p>
 * This link represents a relationship between a composite type and one of its
 * projections. It states that the projection in question is an atom in itself 
 * and therefore must be recorded as a <code>HGAtomRef</code> in all values
 * of this composite type. The <code>mode</code> of the atom reference projection
 * is the sole attribute of the relationship.
 * </p>
 *
 * <p>
 * The link assumes that projections are somehow represented as hypergraph
 * atoms. This is the case, for instance, with <code>Slot</code>s of the
 * <code>Record</code> type. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class AtomProjection extends HGPlainLink
{
	private HGAtomRef.Mode mode;
	
	
	public AtomProjection(HGHandle [] targetSet)
	{
		super(targetSet);
	}
	
	/**
	 * <p>
	 * Construct an atom projection link.
	 * </p>
	 * 
	 * @param type The handle to a <code>HGCompositeType</code>.
	 * @param projection The handle to the atom projection. 
	 * @param mode The mode of the atom reference to be used when managing atoms
	 * of the composite type.
	 */
	public AtomProjection(HGHandle type, HGHandle projection, HGAtomRef.Mode mode)
	{
		super(new HGHandle[] {type, projection});
		this.mode = mode;
	}
	
	public HGHandle getType()
	{
		return getTargetAt(0);
	}
	
	public HGHandle getProjection()
	{
		return getTargetAt(1);
	}
	
	public HGAtomRef.Mode getMode()
	{
		return mode;
	}
	
	public void setMode(HGAtomRef.Mode mode)
	{
		this.mode =  mode;
	}
}