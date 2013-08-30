package org.hypergraphdb.query;

import java.util.ArrayList;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hypergraphdb.query.cond2qry.ConditionToQuery;
import org.hypergraphdb.query.cond2qry.ContractConjunction;

/**
 * <p>
 * Holds a set of configuration settings for the query sub-system of a {@link HyperGraph}
 * instance.
 * </p>
 * 
 * <p>
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGQueryConfiguration
{
    private boolean parallelExecution = false;
    private HashMap<Class<? extends HGQueryCondition>, List<QueryCompile.Expand>> expandTransforms = 
            new HashMap<Class<? extends HGQueryCondition>, List<QueryCompile.Expand>>();
    private HashMap<Class<? extends HGQueryCondition>, List<QueryCompile.Contract>> contractTransforms = 
        new HashMap<Class<? extends HGQueryCondition>, List<QueryCompile.Contract>>();
    private Map<Class<? extends HGQueryCondition>, ConditionToQuery<?>> translators =
            new HashMap<Class<? extends HGQueryCondition>, ConditionToQuery<?>>();
    
    public HGQueryConfiguration()
    {
        addContractTransform(And.class, new ContractConjunction.TypeValueContract());        
        addContractTransform(And.class, new ContractConjunction.ApplyByPartIndex());
        addContractTransform(And.class, new ContractConjunction.ApplyByTargetIndex());
    }
    
    @SuppressWarnings("unchecked")
    public <T> ConditionToQuery<T> compiler(Class<?> type)
    {
        return (ConditionToQuery<T>)translators.get(type);
    }
    
    public HGQueryConfiguration addCompiler(Class<? extends HGQueryCondition> type, ConditionToQuery<?> compiler)
    {
        translators.put(type, compiler);
        return this;
    }
    
    public HGQueryConfiguration addExpandTransform(Class<? extends HGQueryCondition> type, QueryCompile.Expand transform)
    {
          List<QueryCompile.Expand> L = expandTransforms.get(type);
          if (L == null)
          {
            L = new ArrayList<QueryCompile.Expand>();
            expandTransforms.put(type, L);  
          }
        L.add(transform);
        return this;
    }

    public HGQueryConfiguration addContractTransform(Class<? extends HGQueryCondition> type, QueryCompile.Contract transform)
    {
      List<QueryCompile.Contract> L = contractTransforms.get(type);
      if (L == null)
      {
        L = new ArrayList<QueryCompile.Contract>();
        contractTransforms.put(type, L);    
      }
      L.add(transform);
      return this;
    }

    @SuppressWarnings("unchecked")
        public List<QueryCompile.Expand> getExpandTransforms(Class<? extends HGQueryCondition> type)
    {
        List<QueryCompile.Expand> L = expandTransforms.get(type);
        return L == null ? Collections.EMPTY_LIST : L;
    }

    @SuppressWarnings("unchecked")
        public List<QueryCompile.Contract> getContractTransforms(Class<? extends HGQueryCondition> type)
    {
        List<QueryCompile.Contract> L = contractTransforms.get(type);
        return L == null ? Collections.EMPTY_LIST : L;
    }

    public boolean isParallelExecution()
    {
        return parallelExecution;
    }

    public void setParallelExecution(boolean parallelExecution)
    {
        this.parallelExecution = parallelExecution;
    }    
}