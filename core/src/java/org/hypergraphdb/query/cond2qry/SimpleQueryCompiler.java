package org.hypergraphdb.query.cond2qry;

import java.util.concurrent.Callable;

import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.transaction.HGTransactionConfig;
import org.hypergraphdb.transaction.TransactionIsReadonlyException;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.VarContext;

public class SimpleQueryCompiler implements QueryCompiler
{
    public <ResultType> HGQuery<ResultType> compile(HyperGraph graph, HGQueryCondition condition)
    {
//        try
//        {
//            return graph.getTransactionManager().ensureTransaction(new Callable<HGQuery<ResultType>>() {
//                public HGQuery<ResultType> call()
//                {
//                    return compileProcess(condition);
//                }
//            }, HGTransactionConfig.READONLY);
//        }
//        catch (Throwable t)
//        {
//            if (HGUtils.getRootCause(t) instanceof TransactionIsReadonlyException)
//            {
//                if (this.hasVarContext) // it would have been popped when the transaction was aborted, so we push a new one
//                    VarContext.pushFrame(this.ctx);                     
//                return graph.getTransactionManager().ensureTransaction(new Callable<HGQuery<ResultType>>() {
//                    public HGQuery<ResultType> call()
//                    {
//                        return compileProcess(condition);
//                    }});
//            }
//            else if (t instanceof RuntimeException)
//                throw (RuntimeException)t;
//            else if (t instanceof Error)
//                throw (Error)t;
//            else 
//                throw new RuntimeException(t);
//        }
//
        return null;
    }
}
