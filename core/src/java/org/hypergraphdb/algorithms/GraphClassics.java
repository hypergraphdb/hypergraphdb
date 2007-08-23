package org.hypergraphdb.algorithms;

import java.util.*;

import org.hypergraphdb.*;
import org.hypergraphdb.atom.HGAtomSet;
import org.hypergraphdb.util.Mapping;

/**
 * 
 * <p>
 * A collection of classical graph algorithms implemented within the HyperGraphDB framework.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class GraphClassics
{
	/**
	 * <p>
	 * Implements Dijkstra's algorithm for finding the shortest path between two
	 * nodes (i.e. atoms). The method returns the distance between <code>start</code>
	 * <code>goal</code> or <code>null</code> if the two atoms are not connected.  
	 * </p>
	 * 
	 * <p>
	 * The method allows you to optionally pass your own main data structures 
	 * for use in the algorithm's implementation. If you pass <code>null</code> as the
	 * value of a data structure, the method will create and use its own. Thus,
	 * if you care about the actual paths computed and/or all distances between
	 * nodes on those paths, you should provide your own <code>distanceMatrix</code>
	 * and <code>predecessorMatrix</code>.     
	 * </p>
	 * 
	 * <p>
	 * The <code>weight</code> mapping argument represents a function the computes
	 * the weight of a given link. If you pass <code>null</code>, a weight of 1
	 * will be used for all links. Note that this mapping cannot return negative 
	 * values. Dijkstra's algorithms assumes non-negative weights. If the weights
	 * of your graph can be negative, use the <code>bellman_ford</code> instead.
	 * </p>
	 * 
	 * @param start
	 * @param goal
	 * @param adjencyGenerator
	 * @param weight The function that computes that weight of a link for the purposes
	 * of measuring the distance between nodes. If <code>null</code>, the constant 
	 * function 1 will be used.
	 * @param distanceMatrix The data structure holding the computing distances between
	 * the <code>start</code> atom and all other atoms encountered during the search. Only
	 * <code>put</code> and <code>get</code> are used so you can provide an implementation
	 * that only implements those two methods. If <code>null</code> is passed, a new
	 * temporary <code>HashMap</code> will be instantiated and used throughout the search.
	 * @param predecessorMatrix A map storing the predecessor atoms computed during
	 * the search. Again, only <code>put</code> and <code>get</code> are used here. If
	 * <code>null</code>, the predecessor will not be stored anywhere. 
	 * @return The distance between <code>start</code> and <code>goal</code> or
	 * <code>null</code> if <code>start</code> is unreachable from <code>goal</code>.
	 */
	public 	Double dijkstra(final HGHandle start, 
		 			    	final HGHandle goal, 
		 			    	final HGALGenerator adjencyGenerator,
		 			    	Mapping<HGHandle, Double> weight,
		 			    	Map<HGHandle, Double> distanceMatrix,
		 			    	Map<HGHandle, HGHandle> predecessorMatrix)
	{
		final Map<HGHandle, Double> dm = 
			distanceMatrix == null ? new HashMap<HGHandle, Double>() : distanceMatrix;		
		Comparator<HGHandle> comp = new Comparator<HGHandle>()
		{
			public int compare(HGHandle left, HGHandle right)
			{
				Double l = dm.get(left);
				Double r = dm.get(right);
				if (l == null)
					if (r == null)
						return 0;
					else
						return 1;
				else if (r == null)
					return -1;
				else
					return l.compareTo(r); 
			}
		};
		if (weight == null)
			weight = new Mapping<HGHandle, Double>() { public Double eval(HGHandle link) { return 1.0; } };
			
		HGAtomSet settled = new HGAtomSet(); 
		PriorityQueue<HGHandle> unsettled = new PriorityQueue<HGHandle>(11, comp);
		unsettled.add(start);
		for (HGHandle a = unsettled.poll(); a != null; a = unsettled.poll())
		{
			if (a.equals(goal))
				return dm.get(goal);
			settled.add(a);
			HGSearchResult<HGHandle> neighbors = adjencyGenerator.generate(a);
			double weightCurrent = dm.get(a).doubleValue();
			while (neighbors.hasNext())
			{
				HGHandle n = neighbors.next();
				if (settled.contains(n))
					continue;
				Double weightN = distanceMatrix.get(n);
				Double weightAN = weight.eval(adjencyGenerator.getCurrentLink());
				if (weightN == null || 
					weightN > weightCurrent + weightAN)
				{
					dm.put(n, weightCurrent + weightAN);
					if (predecessorMatrix != null)
						predecessorMatrix.put(n, a);
					unsettled.add(n);
				}
			}
			neighbors.close();
		}
		return null;
	}

	public void bellman_ford()
	{
		
	}
	
	public void a_star()
	{
	}
	
	public void johnson()
	{
		
	}
	
	public void floyd_warshall()
	{
		
	}
	
	public void prim()
	{
		
	}
	
	public void kruskall()
	{
		
	}
}