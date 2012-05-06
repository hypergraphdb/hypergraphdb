package org.hypergraphdb.query;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.util.RelativePosition;

/**
 * <p>
 * A <code>PositionedLinkCondition</code> constraints the query result set to links pointing to a target set
 * of atoms and whose position is consistent with the request. The target set is specified when the condition
 * is constructed through an array of <code>HGHandle</code>s.
 * </p>
 * 
 * @author Alain Picard
 */
public class PositionedIncidentCondition implements HGQueryCondition, HGAtomPredicate {
	private HGHandle target = null;
	private RelativePosition position = null;

	public PositionedIncidentCondition() {
	}

	public PositionedIncidentCondition(HGHandle target, RelativePosition position) {
		if (target == null)
			throw new HGException("PositionedLinkCondition instantiated with a null target.");
		this.target = target;
		this.position = position;
	}

	public HGHandle getTarget() {
		return target;
	}

	public void setTarget(HGHandle target) {
		this.target = target;
	}

	public RelativePosition getPosition() {
		return position;
	}

	public void setPosition(RelativePosition position) {
		this.position = position;
	}

	/**
	 * <p>
	 * Return <code>true</code> if <code>handle</code> points to a link whose target set is a superset of this
	 * condition's <code>targetSet</code>.
	 * </p>
	 */
	// TODO: this assumes that there are no duplicates in A. Not sure whether
	// it should be forbidden in HyperGraph for a link to contains duplicates.
	// Surely, it doesn't make sense for unordered links - they are just sets. 
	// However, ordered ones might be tricky since they are essentially lists
	// (<=> sets of [position, element] pairs).
	public boolean satisfies(HyperGraph hg, HGHandle handle) {
		HGPersistentHandle targetPHandle = hg.getPersistentHandle(target);

//		System.out.println("[" + Thread.currentThread().getName() + "]trying to satisfy " + handle + " for " + target);
		
		// If the atom corresponding to 'handle' is already in the cache, there is no point 
		// fetching it from permanent storage. Otherwise, there's no point caching the actual atom...
		if (hg.isLoaded(handle)) {
			Object atom = hg.get(handle);
			if (!(atom instanceof HGLink))
				return false;
			HGLink link = (HGLink)atom;
			
			switch (position) {
			case FIRST:
				return link.getTargetAt(0).equals(targetPHandle);
			case LAST:
				return link.getTargetAt(link.getArity() -1).equals(targetPHandle);
			case NOT_FIRST:
				for (int i = 1; i < link.getArity(); i++) {  //start at 2nd 
					if (link.getTargetAt(i).equals(targetPHandle))
						return true;
				}
				return false;
			case NOT_LAST:
				for (int i = 0; i < (link.getArity() -1); i++) {  //stop 1 short 
					if (link.getTargetAt(i).equals(targetPHandle))
						return true;
				}
				return false;
				
			default:
				return false;
			}
		}
		else {
			HGPersistentHandle[] A = hg.getStore().getLink(hg.getPersistentHandle(handle));
			switch (position) {
			case FIRST:
				return A[2].equals(targetPHandle);
			case LAST:
				return A[A.length-1].equals(targetPHandle);
			case NOT_FIRST:
				for (int i = 3; i < A.length; i++) { //start at 2nd (4th slot or 2nd link)
					if (A[i].equals(targetPHandle))
						return true;
				}
				return false;
			case NOT_LAST:
				for (int i = 2; i < (A.length -1); i++) { //stop 1 short
					if (A[i].equals(targetPHandle))
						return true;
				}
				return false;
				
			default:
				return false;
			}
		}
	}

	public int hashCode() {
		int hash = 7;
		hash = 31 * hash + target.hashCode();
		hash = 31 * hash + position.hashCode();
    return hash;
	}

	public boolean equals(Object x) {
		if (!(x instanceof PositionedIncidentCondition))
			return false;
		else {
			PositionedIncidentCondition other = (PositionedIncidentCondition)x;
			return other.target.equals(target) && other.position.equals(position);
		}
	}

	public String toString() {
		StringBuffer result = new StringBuffer("linksTo(");
		result.append(target.toString());
		result.append(',');
		result.append(position.toString());
		result.append(")");
		return result.toString();
	}
}
