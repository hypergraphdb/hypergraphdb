package org.hypergraphdb;

class QueryMetaData implements Cloneable
{
	static QueryMetaData EMPTY = new QueryMetaData(true, true, 0, 0);
	static QueryMetaData MISTERY = new QueryMetaData(false, false);
	static QueryMetaData ORDERED = new QueryMetaData(true, false);
	static QueryMetaData RACCESS = new QueryMetaData(false, true);
	static QueryMetaData ORACCESS = new QueryMetaData(true, true);
	
	boolean ordered;
	boolean randomAccess;
	long sizeLB = 0;
	long sizeUB = Long.MAX_VALUE;
	long sizeExpected = -1;
	
	// The predicate cost has the following intended meaning:
	// x=-1 means no predicate available
	// x=0 means no DB access is ever performed
	// x=0.5 means a single DB access may be performed (unless data is in cache)
	// x=1 means one and only one DB access is performed
	// 1 < x < 2 means at least one, but maybe two DB accesses are performed etc.
	double predicateCost = -1; // condition is not a predicate...
	boolean predicateOnly = false;
	
	QueryMetaData(boolean ordered, boolean randomAccess)
	{
		this.ordered = ordered;
		this.randomAccess = randomAccess;
	}

	QueryMetaData(boolean ordered, boolean randomAccess, double predicateCost)
	{
		this.ordered = ordered;
		this.randomAccess = randomAccess;
		this.predicateCost = predicateCost;
	}
	
	QueryMetaData(boolean ordered, boolean randomAccess, long sizeLB, long sizeUB)
	{
		this(ordered, randomAccess);
		this.sizeLB = sizeLB;
		this.sizeUB = sizeUB;
	}

	QueryMetaData(boolean ordered, boolean randomAccess, long sizeLB, long sizeUB, double predicateCost)
	{
		this(ordered, randomAccess, predicateCost);
		this.sizeLB = sizeLB;
		this.sizeUB = sizeUB;
	}
	
	QueryMetaData(boolean ordered, boolean randomAccess, long sizeLB, long sizeUB, long sizeExpected)
	{
		this(ordered, randomAccess, sizeLB, sizeUB);
		this.sizeExpected = sizeExpected;
	}

	QueryMetaData(boolean ordered, boolean randomAccess, long sizeLB, long sizeUB, long sizeExpected, double predicateCost)
	{
		this(ordered, randomAccess, sizeLB, sizeUB, predicateCost);
		this.sizeExpected = sizeExpected;
	}
	
	long getSizeExpected()
	{
		return sizeExpected < 0 ? (sizeUB - sizeLB)/2 : sizeExpected;
	}
	
	public QueryMetaData clone()
	{
		return new QueryMetaData(ordered, randomAccess, sizeLB, sizeUB, sizeExpected, predicateCost); 
	}
}