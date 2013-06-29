package org.hypergraphdb.query.cond2qry;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.Or;
import org.hypergraphdb.query.QueryCompile;
import org.hypergraphdb.query.impl.UnionQuery;

public class OrToQuery<T> implements ConditionToQuery<T>
{
    public HGQuery<T> getQuery(HyperGraph graph, HGQueryCondition c)
    {
        Or or = (Or)c;
        if (or.size() == 0)
            return HGQuery.NOP();
        else if (or.size() == 1)
            return QueryCompile.translate(graph, or.get(0));
        
        // TODO - we need to do better, even for this sloppy algorithm, we can
        // can try to factor out common conditions in conjunctions, make sure 
        // all conjunction end up in a treatable form (ordered or randomAccess) etc.
        
        HGQuery<T> q1 = QueryCompile.translate(graph, or.get(0));
        if (q1 == null)
            throw new HGException("Untranslatable condition " + or.get(0));
        HGQuery<T> q2 = QueryCompile.translate(graph, or.get(1));
        if (q2 == null)
            throw new HGException("Untranslatable condition " + or.get(1));
        UnionQuery<T> result = new UnionQuery<T>(q1, q2);
        result.setHyperGraph(graph);
        for (int i = 2; i < or.size(); i++)
        {
            q1 = QueryCompile.translate(graph, or.get(i));
            if (q1 == null)
                throw new HGException("Untranslatable condition " + or.get(i));                 
            result = new UnionQuery<T>(result, q1);
            result.setHyperGraph(graph);
        }
        return result;
    }
    public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
    {
        QueryMetaData x = QueryMetaData.ORDERED.clone(c);
        boolean ispredicate = true;
        x.predicateCost = 0;
        for (HGQueryCondition sub : ((Or)c))
        {
            if (! (sub instanceof HGAtomPredicate))
                ispredicate = false;                    
            ConditionToQuery<T> transformer = QueryCompile.translator(hg, sub.getClass());
            if (transformer == null)
            {
                if (! (sub instanceof HGAtomPredicate))
                    throw new HGException("Condition " + sub + " is not query translatable, nor a predicate.");
                else 
                {
                    x.ordered = false;
                    x.randomAccess = false;
                    continue;
                }
            }
            QueryMetaData subx = transformer.getMetaData(hg, sub);
            ispredicate = ispredicate && subx.predicateCost > -1;
            x.predicateCost += subx.predicateCost;
            x.ordered = x.ordered && subx.ordered;
            x.randomAccess = x.randomAccess && subx.randomAccess;
        }
        if (!ispredicate)
            x.predicateCost = -1;
        else
            x.predicateCost  /= ((Or)c).size();
        return x;
    }
}