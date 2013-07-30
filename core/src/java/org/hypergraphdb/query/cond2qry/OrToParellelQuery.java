package org.hypergraphdb.query.cond2qry;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

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

    private static class ParallelUnionQuery<T> extends HGQuery<T> implements Iterable<HGQuery<T>>
    {
        private HGQuery<T> left, right;
        
        /**
         * <p>Construct a union of two queries.</p>
         * 
         * @param left One of the two queries. May not be <code>null</code>.
         * @param right The other of the two queries. May not be <code>null</code>.
         */
        public ParallelUnionQuery(HGQuery<T> left, HGQuery<T> right, HyperGraph graph)
        {
            this.left = left;
            this.right = right;
            this.setHyperGraph(graph);
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
                    lasync = new AsyncSearchResultImpl(graph, leftResult);
                AsyncSearchResult rasync;
                if (rightResult instanceof AsyncSearchResult)
                    rasync = (AsyncSearchResult) rightResult;
                else
                    rasync = new AsyncSearchResultImpl(graph, rightResult);
                return new UnionResultAsync(lasync, rasync);
            }
        }
        
        public Iterator<HGQuery<T>> iterator()
        {
            return Arrays.asList(left,right).iterator();
        }        
    }
    
    @SuppressWarnings("unchecked")    
    public HGQuery<T> getQuery(HyperGraph graph, HGQueryCondition condition)
    {
        Or or = (Or)condition;
        if (or.size() == 0)
            return (HGQuery<T>)HGQuery.NOP();
        else if (or.size() == 1)
            return QueryCompile.translate(graph, or.get(0));
        
        // We partition according to the number of available processors. We don't
        // a separate task for each clause because there could be hundreds of
        // them, leading to unacceptable overhead.
        int nbprocessors = Runtime.getRuntime().availableProcessors();
        Or [] tasks = new Or[nbprocessors];
        int slot = 0;
        for (HGQueryCondition sub : or)
        {
            if (tasks[slot] == null)
                tasks[slot] = new Or();
            tasks[slot].add(sub);
            slot = (slot+1) % tasks.length;
        }
        
        ConditionToQuery<T> sequentialOr = new OrToQuery<T>();
        
        HGQuery<T> q1 = sequentialOr.getQuery(graph, tasks[0]);
        if (tasks[1] == null)
            return q1;
        HGQuery<T> q2 = sequentialOr.getQuery(graph, tasks[1]);
        ParallelUnionQuery<T> result = new ParallelUnionQuery<T>(q1, q2, graph);
        for (int i = 2; i < tasks.length; i++)
        {
            q1 = sequentialOr.getQuery(graph, tasks[i]);
            result = new ParallelUnionQuery<T>(result, q1, graph);
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