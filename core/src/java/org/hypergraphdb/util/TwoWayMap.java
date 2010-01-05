/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * A bi-directional map <code>X <-> Y</code>. For lack of better terms
 * (yes, we considered "domain" and "range") the map is between a set of 
 * Xs and a set of Ys.
 * </p>
 * 
 * <p>
 * Note that this class is <b>not</b> thread-safe. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <X>
 * @param <Y>
 */
public class TwoWayMap<X,Y>
{
	private Map<X,Y> xtoy = new HashMap<X,Y>();
	private Map<Y,X> ytox = new HashMap<Y,X>();
	
	public void add(X x, Y y)
	{
		xtoy.put(x, y);
		ytox.put(y, x);
	}
	
	public Iterator<X> xiterator() { return xtoy.keySet().iterator(); }
	public Iterator<Y> yiterator() { return ytox.keySet().iterator(); }
	public Iterator<Pair<X,Y>> xyiterator() 
	{ 
		final Iterator<Map.Entry<X, Y>> i = xtoy.entrySet().iterator();
		return new Iterator<Pair<X,Y>>()
		{
			public void remove() 
			{
				i.remove();
			}
			public boolean hasNext() 
			{ 
				return i.hasNext(); 
			}
			public Pair<X,Y> next() 
			{ 
				Map.Entry<X, Y> e = i.next(); 
				return new Pair<X,Y>(e.getKey(), e.getValue()); 
			}
		};
	}
	public Set<Y> getYSet() { return ytox.keySet(); }
	public Set<X> getXSet() { return xtoy.keySet(); }
	public Y removeX(X x) { Y y = xtoy.remove(x); if (y != null) ytox.remove(y); return y;}
	public X removeY(Y y) { X x = ytox.remove(y); if (x != null) xtoy.remove(x); return x;}
	public Y getY(X x) { return xtoy.get(x); }
	public X getX(Y y) { return  ytox.get(y); }
	public boolean containsX(X x) { return xtoy.containsKey(x); }
	public boolean containsY(Y y) { return ytox.containsKey(y); }
	public boolean isEmtpy() { return xtoy.isEmpty(); }
	public void clear() { xtoy.clear(); ytox.clear(); }
}
