package org.hypergraphdb.query.cond2qry;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.Or;
import org.hypergraphdb.query.QueryCompile;
import org.hypergraphdb.query.impl.AsyncSearchResult;
import org.hypergraphdb.query.impl.AsyncSearchResultImpl;
import org.hypergraphdb.query.impl.UnionResultAsync;

public class OrToParellelQuery<T> implements ConditionToQuery<T>
{

    private static class ParallelUnionQuery<T> extends HGQuery<T>
    {
        private HGQuery<T> left, right;
        
        /**
         * <p>Construct a union of two queries.</p>
         * 
         * @param left One of the two queries. May not be <code>null</code>.
         * @param right The other of the two queries. May not be <code>null</code>.
         */
        public ParallelUnionQuery(HGQuery<T> left, HGQuery<T> right)
        {
            this.left = left;
            this.right = right;
        }
        
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public HGSearchResult<T> execute()
        {
            HGSearchResult<T> leftResult = left.execute();            
            HGSearchResult<T> rightResult = right.execute();        
            if (!leftResult.hasNext() && !rightResult.hasNext())
            {
                leftResult.close();
                rightResult.close();
                return (HGSearchResult)HGSearchResult.EMPTY;
            }
            else if (!leftResult.hasNext())
            {
                leftResult.close();
                return rightResult;
            }
            else if (!rightResult.hasNext())
            {
                rightResult.close();
                return leftResult;
            }
            else
            {
                AsyncSearchResult lasync;
                if (leftResult instanceof AsyncSearchResult)
                    lasync = (AsyncSearchResult) leftResult;
                else
                    lasync = new AsyncSearchResultImpl(left.getHyperGraph(), leftResult);
                AsyncSearchResult rasync;
                if (rightResult instanceof AsyncSearchResult)
                    rasync = (AsyncSearchResult) rightResult;
                else
                    rasync = new AsyncSearchResultImpl(right.getHyperGraph(), rightResult);
                return new UnionResultAsync(lasync, rasync);
            }
        }
    }
    
    public HGQuery<T> getQuery(HyperGraph graph, HGQueryCondition condition)
    {
        Or or = (Or)condition;
        if (or.size() == 0)
            return (HGQuery<T>)HGQuery.NOP();
        else if (or.size() == 1)
            return QueryCompile.translate(graph, or.get(0));
        
        HGQuery<T> q1 = QueryCompile.translate(graph, or.get(0));
        if (q1 == null)
            throw new HGException("Untranslatable condition " + or.get(0));
        HGQuery<T> q2 = QueryCompile.translate(graph, or.get(1));
        if (q2 == null)
            throw new HGException("Untranslatable condition " + or.get(1));
        ParallelUnionQuery<T> result = new ParallelUnionQuery(q1, q2);
        for (int i = 2; i < or.size(); i++)
        {
            q1 = QueryCompile.translate(graph, or.get(i));
            if (q1 == null)
                throw new HGException("Untranslatable condition " + or.get(i));                 
            result = new ParallelUnionQuery(result, q1);
        }
        return result;
    }

    public QueryMetaData getMetaData(HyperGraph graph,
                                     HGQueryCondition condition)
    {
        QueryMetaData x = QueryMetaData.ORDERED.clone(condition);
        return x;
    }
}