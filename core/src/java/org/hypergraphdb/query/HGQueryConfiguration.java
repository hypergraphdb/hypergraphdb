package org.hypergraphdb.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hypergraphdb.query.cond2qry.ConditionToQuery;
import org.hypergraphdb.util.Mapping;

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
    private List<Mapping<HGQueryCondition, HGQueryCondition>> transforms = 
            new ArrayList<Mapping<HGQueryCondition, HGQueryCondition>>();
    private Map<Class<? extends HGQueryCondition>, ConditionToQuery<?>> translators =
            new HashMap<Class<? extends HGQueryCondition>, ConditionToQuery<?>>();
    
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
    
    // TODO: probably need a separate interface for those transforms, one that takes the graph
    // as a method parameter. A plain mapping means that the graph has to be a member variable
    // initialized already. This would be fine, except that it's weird during configuration
    // time when the graph has to be created and only then configuration can be completed. 
    // More importantly, if there's action during the HGOpenedEvent, it won't have access
    // to query configurations.
    public HGQueryConfiguration addTransform(Mapping<HGQueryCondition, HGQueryCondition> transform)
    {
        transforms.add(transform);
        return this;
    }
    
    public List<Mapping<HGQueryCondition, HGQueryCondition>> getTransforms()
    {
        return transforms;
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