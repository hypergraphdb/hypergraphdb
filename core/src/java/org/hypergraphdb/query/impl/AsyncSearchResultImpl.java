package org.hypergraphdb.query.impl;

import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.transaction.HGTransactionContext;

/**
 * <p>
 * Default implementation of {@link AsyncSearchResult} based on an underlying 
 * {@link HGSearchResult} where each operation is submitted as a task to the 
 * {@link HGEnvironment#executor()}.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <T>
 */
public class AsyncSearchResultImpl<T> implements AsyncSearchResult<T>
{
    HyperGraph graph;
    HGSearchResult<T> rs;
        
    public AsyncSearchResultImpl(HyperGraph graph, HGSearchResult<T> rs)
    {
        this.graph = graph;
        this.rs = rs;
    }
    
    @Override
    public boolean hasPrev()
    {
        return rs.hasPrev();
    }

    public Future<Boolean> hasPrevAsync()
    {
        final HGTransactionContext tcontext = graph.getTransactionManager().getContext();
        Callable<Boolean> c = new Callable<Boolean>() { public Boolean call() {
            graph.getTransactionManager().threadAttach(tcontext);
            try
            {
                return rs.hasPrev();
            }
            finally
            {
                graph.getTransactionManager().threadDetach();
            }
        } };
        return HGEnvironment.executor().submit(c);
    }
    
    @Override
    public Future<T> prev()
    {
        final HGTransactionContext tcontext = graph.getTransactionManager().getContext();        
        Callable<T> c = new Callable<T>() { public T call() {
            graph.getTransactionManager().threadAttach(tcontext);
            try
            {
                return rs.prev();
            }
            finally
            {
                graph.getTransactionManager().threadDetach();
            }
        } };
        return HGEnvironment.executor().submit(c);
    }

    @Override
    public boolean hasNext()
    {
        return rs.hasNext();
    }
    
    public Future<Boolean> hasNextAsync()
    {
        final HGTransactionContext tcontext = graph.getTransactionManager().getContext();        
        Callable<Boolean> c = new Callable<Boolean>() { public Boolean call() {
            graph.getTransactionManager().threadAttach(tcontext);
            try
            {
                return rs.hasNext();
            }
            finally
            {
                graph.getTransactionManager().threadDetach();
            }
        } };
        return HGEnvironment.executor().submit(c);
    }

    @Override
    public Future<T> next()
    {
        if (!hasNext())
            throw new NoSuchElementException();
        final HGTransactionContext tcontext = graph.getTransactionManager().getContext();        
        Callable<T> c = new Callable<T>() { public T call() {
            graph.getTransactionManager().threadAttach(tcontext);
            try
            {
                return rs.next();
            }
            finally
            {
                graph.getTransactionManager().threadDetach();
            }
        } };
        return HGEnvironment.executor().submit(c);
    }

    @Override
    public void remove()
    {
        rs.remove();
    }

    @Override
    public Future<T> current()
    {
        final HGTransactionContext tcontext = graph.getTransactionManager().getContext();        
        Callable<T> c = new Callable<T>() { public T call() {
            graph.getTransactionManager().threadAttach(tcontext);
            try
            {
                return rs.current();
            }
            finally
            {
                graph.getTransactionManager().threadDetach();
            }
        } };
        return HGEnvironment.executor().submit(c);        
    }

    @Override
    public void close()
    {
        rs.close();
    }

    @Override
    public boolean isOrdered()
    {
        return rs.isOrdered();
    }
}