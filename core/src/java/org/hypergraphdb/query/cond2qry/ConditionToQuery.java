/**
 * 
 */
package org.hypergraphdb.query.cond2qry;

import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGQueryCondition;

public interface ConditionToQuery
{
	HGQuery<?> getQuery(HyperGraph graph, HGQueryCondition condition);
	QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition condition);
}