package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;

/**
 * <p>
 * This queries simply scans all elements in a given index. The result of
 * <code>execute</code> is actually <code>HGRandomAccessResult</code>. One can
 * scan either the keys or the values of the <code>HGIndex</code> based on
 * the <code>returnKeys</code> boolean constructor parameter.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class IndexScanQuery extends HGQuery 
{
	private HGIndex idx;
	private boolean returnKeys = false;
	
	public IndexScanQuery(HGIndex idx, boolean returnKeys)
	{
		this.idx = idx;
		this.returnKeys = returnKeys;
	}
	
	@Override
	public HGSearchResult execute() 
	{
		return returnKeys ? idx.scanKeys() : idx.scanValues();
	}
}