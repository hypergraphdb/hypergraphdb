/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.algorithms;

import java.util.*;

import org.hypergraphdb.*;
import org.hypergraphdb.atom.HGAtomSet;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.util.Mapping;
import org.hypergraphdb.util.Pair;

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
	 * 
	 * <p>
	 * Detect whether a sub-graph has cycles. Links are specified through an
	 * <code>HGALGenerator</code> as usual.
	 * </p>
	 *
	 * @param root The starting point for sub-graph exploration.
	 * @param adjencyGenerator Generator for atoms adjacent to the current node being examined.
	 * @return <code>true</code> if the sub-graph has cycles and <code>false</code> otherwise.
	 */
	public static boolean hasCycles(final HGHandle root, final HGALGenerator adjencyGenerator)
	{
		HGAtomSet visited = new HGAtomSet();
		Queue<HGHandle> to_explore = new LinkedList<HGHandle>();
		to_explore.add(root);
		while (!to_explore.isEmpty())
		{
			HGHandle next = to_explore.remove();
			visited.add(next);
			HGSearchResult<Pair<HGHandle, HGHandle>> rs = adjencyGenerator.generate(next);
			try
			{
				while (rs.hasNext())
				{
				    Pair<HGHandle, HGHandle> x = rs.next();
					if (visited.contains(x.getSecond()))
						return true;
					to_explore.add(x.getSecond());
				}
			}
			finally
			{
				rs.close();
			}
		}
		return false;
	}
	
	/**
	 * <p>
	 * Simplified interface to Dijkstra's algorithm - calls the full version with
	 * the remaining arguments set to <code>null</code>.
	 * </p>
	 * 
	 * @param start 
	 * @param goal
	 * @param adjencyGenerator
	 * @return The number of edges b/w <code>start</code> and <code>goal</code> or
	 * <code>null</code> if they are not connected.
	 */
	public static Double dijkstra(final HGHandle start, final HGHandle goal, final HGALGenerator adjencyGenerator)
	{
		return dijkstra(start, goal, adjencyGenerator, null, null, null);
	}
	
	/**
	 * <p>
	 * Implements Dijkstra's algorithm for finding the shortest path between two
	 * nodes (i.e. atoms). The method returns the distance between <code>start</code>
	 * and <code>goal</code> or <code>null</code> if the two atoms are not connected.  
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
	 * The <code>weight</code> mapping argument represents a function that computes
	 * the weight of a given link. If you pass <code>null</code>, a weight of 1
	 * will be used for all links. Note that this mapping cannot return negative 
	 * values. Dijkstra's algorithms assumes non-negative weights. If the weights
	 * of your graph can be negative, use the <code>bellman_ford</code> instead.
	 * </p>
	 * 
	 * @param start
	 * @param goal
	 * @param adjacencyGenerator
	 * @param weight The function that computes that weight of a link for the purposes
	 * of measuring the distance between nodes. If <code>null</code>, the constant 
	 * function 1 will be used.
	 * @param distanceMatrix The data structure holding the computed distances between
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
	public static Double dijkstra(final HGHandle start, 
				 			       final HGHandle goal, 
				 			       final HGALGenerator adjacencyGenerator,
				 			       Mapping<HGHandle, Double> weight,
				 			       Map<HGHandle, Double> distanceMatrix,
				 			       Map<HGHandle, HGHandle> predecessorMatrix)
	{
		final Map<HGHandle, Double> dm = 
			distanceMatrix == null ? new HashMap<HGHandle, Double>() : distanceMatrix;
		dm.put(start, 0.0);
		Comparator<HGHandle> comp = new Comparator<HGHandle>()
		{
			private int compareHandles(HGHandle left, HGHandle right)
			{				
				HGPersistentHandle x = left instanceof HGPersistentHandle ?
						(HGPersistentHandle)left : ((HGLiveHandle)left).getPersistent();
				HGPersistentHandle y = right instanceof HGPersistentHandle ?
						(HGPersistentHandle)right : ((HGLiveHandle)right).getPersistent();
				return x.compareTo(y);				
			}
			
			public int compare(HGHandle left, HGHandle right)
			{
				Double l = dm.get(left);
				Double r = dm.get(right);
				if (l == null)
					if (r == null)
						return compareHandles(left, right);
					else
						return 1;
				else if (r == null)
					return -1;
				else
				{
					int c = l.compareTo(r);
					if (c == 0)
						c = compareHandles(left, right);
					return c;
				}
			}
		};
		if (weight == null)
			weight = new Mapping<HGHandle, Double>() { public Double eval(HGHandle link) { return 1.0; } };
			
		HGAtomSet settled = new HGAtomSet(); 
		TreeSet<HGHandle> unsettled = new TreeSet<HGHandle>(comp);
		unsettled.add(start);
		while (!unsettled.isEmpty())		
		{
			HGHandle a = unsettled.first();
			unsettled.remove(a);
			if (a.equals(goal))
				return dm.get(goal);
			settled.add(a);
			HGSearchResult<Pair<HGHandle, HGHandle>> neighbors = adjacencyGenerator.generate(a);
			double weightCurrent = dm.get(a).doubleValue();
			while (neighbors.hasNext())
			{
				Pair<HGHandle, HGHandle> n = neighbors.next();
				if (settled.contains(n.getSecond()))
					continue;
				Double weightN = dm.get(n.getSecond());
				Double weightAN = weight.eval(n.getFirst());
				if (weightN == null)
				{
					dm.put(n.getSecond(), weightCurrent + weightAN);
					unsettled.add(n.getSecond());
					if (predecessorMatrix != null)
						predecessorMatrix.put(n.getSecond(), a);					
				}
				else if (weightN > weightCurrent + weightAN)
				{
					// new distance found for n, re-insert at appropriate position
					unsettled.remove(n.getSecond());
					dm.put(n.getSecond(), weightCurrent + weightAN);
					unsettled.add(n.getSecond());
					if (predecessorMatrix != null)
						predecessorMatrix.put(n.getSecond(), a);
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
	
	public void prim(HGHandle start, 
					 final HGALGenerator adjencyGenerator, 
					 Mapping<HGHandle, Double> weight,
					 Map<HGHandle, HGHandle> parentMatrix)
	{
		
	}
	
	public void kruskall(Iterator<HGHandle> links,
						 Mapping<HGHandle, Double> weight,
						 Map<HGHandle, HGHandle> parentMatrix)
	{
		
	}
}
