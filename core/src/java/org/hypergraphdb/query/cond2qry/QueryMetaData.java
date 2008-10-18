package org.hypergraphdb.query.cond2qry;

public class QueryMetaData implements Cloneable
{
	public final static QueryMetaData EMPTY = new QueryMetaData(true, true, 0, 0);
	public final static QueryMetaData MISTERY = new QueryMetaData(false, false);
	public final static QueryMetaData ORDERED = new QueryMetaData(true, false);
	public final static QueryMetaData RACCESS = new QueryMetaData(false, true);
	public final static QueryMetaData ORACCESS = new QueryMetaData(true, true);
	
	public boolean ordered;
	public boolean randomAccess;
	public long sizeLB;
	public long sizeUB;
	public long sizeExpected;
	
	// The predicate cost has the following intended meaning:
	// x=-1 means no predicate available
	// x=0 means no DB access is ever performed
	// x=0.5 means a single DB access may be performed (unless data is in cache)
	// x=1 means one and only one DB access is performed
	// 1 < x < 2 means at least one, but maybe two DB accesses are performed etc.
	public double predicateCost = -1; // condition is not a predicate...
	public boolean predicateOnly = false;
	
	public QueryMetaData(boolean ordered, boolean randomAccess)
	{
		this.ordered = ordered;
		this.randomAccess = randomAccess;
		this.sizeLB = 0;
		this.sizeUB = Long.MAX_VALUE;
		this.sizeExpected = -1;
	}

	public QueryMetaData(boolean ordered, boolean randomAccess, double predicateCost)
	{
		this.ordered = ordered;
		this.randomAccess = randomAccess;
		this.predicateCost = predicateCost;
		this.sizeLB = 0;
		this.sizeUB = Long.MAX_VALUE;
		this.sizeExpected = -1;		
	}
	
	public QueryMetaData(boolean ordered, boolean randomAccess, long sizeLB, long sizeUB)
	{
		this.ordered = ordered;
		this.randomAccess = randomAccess;
		this.sizeLB = sizeLB;
		this.sizeUB = sizeUB;
		this.sizeExpected = -1;
	}

	public QueryMetaData(boolean ordered, boolean randomAccess, long sizeLB, long sizeUB, double predicateCost)
	{
		this.ordered = ordered;
		this.randomAccess = randomAccess;		
		this.sizeLB = sizeLB;
		this.sizeUB = sizeUB;
		this.sizeExpected = -1;
		this.predicateCost = predicateCost;
	}
	
	public QueryMetaData(boolean ordered, boolean randomAccess, long sizeLB, long sizeUB, long sizeExpected)
	{
		this.ordered = ordered;
		this.randomAccess = randomAccess;
		this.sizeLB = sizeLB;
		this.sizeUB = sizeUB;
		this.sizeExpected = sizeExpected;
	}

	public QueryMetaData(boolean ordered, boolean randomAccess, long sizeLB, long sizeUB, long sizeExpected, double predicateCost)
	{
		this.ordered = ordered;
		this.randomAccess = randomAccess;		
		this.sizeLB = sizeLB;
		this.sizeUB = sizeUB;
		this.sizeExpected = sizeExpected;
		this.predicateCost = predicateCost;	}
	
	public long getSizeExpected()
	{
		return sizeExpected < 0 ? (sizeUB - sizeLB)/2 : sizeExpected;
	}
	
	public QueryMetaData clone()
	{
		return new QueryMetaData(ordered, randomAccess, sizeLB, sizeUB, sizeExpected, predicateCost); 
	}
}