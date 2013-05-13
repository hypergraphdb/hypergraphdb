package org.hypergraphdb.query.cond2qry;

import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGQueryCondition;

public interface QueryCompiler
{
    public <T> HGQuery<T> compile(HyperGraph graph, HGQueryCondition condition);
}