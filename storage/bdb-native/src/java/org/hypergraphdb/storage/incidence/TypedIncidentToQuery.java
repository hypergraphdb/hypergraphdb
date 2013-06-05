package org.hypergraphdb.storage.incidence;

import org.hypergraphdb.HGHandle;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.cond2qry.ConditionToQuery;
import org.hypergraphdb.query.cond2qry.QueryMetaData;
import org.hypergraphdb.storage.bdb.BDBStorageImplementation;

public class TypedIncidentToQuery implements ConditionToQuery<HGHandle>
{

    public HGQuery<HGHandle> getQuery(final HyperGraph graph, final HGQueryCondition condition)
    {
        final TypedIncidentCondition ti = (TypedIncidentCondition)condition;
        return new HGQuery<HGHandle>() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            public HGSearchResult<HGHandle> execute()
            {
                return (HGSearchResult)((BDBStorageImplementation)graph.getConfig().getStoreImplementation()).getAnnotatedIncidenceResultSet(             
                        ti.getTargetRef().get(), 
                        ti.getTypeRef().get());
            }
        };
    }

    public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition c)
    {
        QueryMetaData x = QueryMetaData.ORACCESS.clone(c);
        x.predicateCost = 1;
        TypedIncidentCondition ic = (TypedIncidentCondition)c;
        if (hg.isVar(ic.getTargetRef()))
        {
            x.sizeExpected = 1000; // incidence sets are usually small...
        }
        else
        {
            final HGPersistentHandle handle = graph.getPersistentHandle(((TypedIncidentCondition)c).getTargetRef().get());        
            x.sizeLB = x.sizeExpected = x.sizeUB = graph.getIncidenceSet(handle).size();
        }
        return x;
    }

}