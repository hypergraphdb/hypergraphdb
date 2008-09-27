package org.hypergraphdb.peer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Cipri Costa
 *
 * Implementors can filter peers that match  a give description. The result of the filter can be 
 * used as a target in any subclass of the <code>PeerRelatedActivity</code> class.
 */
public abstract class PeerFilter 
{
	protected PeerFilterEvaluator evaluator;
	
	private List<Object> targets = new ArrayList<Object>();

	public PeerFilter()
	{
	}
		
	public abstract void filterTargets();

	
	public Iterator<Object> iterator()
	{
		return targets.iterator();
	}

	protected void matchFound(Object target)
	{
		targets.add(target);
	}

	public PeerFilterEvaluator getEvaluator()
	{
		return evaluator;
	}

	public void setEvaluator(PeerFilterEvaluator evaluator)
	{
		this.evaluator = evaluator;
	}

}
