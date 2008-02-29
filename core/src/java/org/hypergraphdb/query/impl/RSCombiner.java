package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGSearchResult;

public interface RSCombiner<T>  extends HGSearchResult<T>
{
	void init(HGSearchResult<T> l, HGSearchResult<T> r);
}
