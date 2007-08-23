package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGSearchResult;

public interface RSCombiner<Left, Right>  extends HGSearchResult
{
	void init(Left l, Right r);
}
