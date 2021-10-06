package org.hypergraphdb.query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.query.cond2qry.QueryMetaData;
import org.hypergraphdb.query.impl.IntersectionQuery;
import org.hypergraphdb.query.impl.PredicateBasedFilter;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;

/**
 * <p>
 * A query object that holds information collected during the compilation process. When a 
 * {@link HGQueryCondition} is transformed into a {@link HGQuery}, it goes through several
 * phases where the original condition and sub-conditions get transformed into other condition
 * objects, as intermediary steps, and eventually into an executable <code>HGQuery</code>.  
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <SearchResult>
 */
public class AnalyzedQuery<SearchResult> extends HGQuery<SearchResult>
{
    public static final String INTERSECTION_THRESHOLD = "intersection-threshold";
    public static final String SCAN_THRESHOLD = "scan-threshold";
    
    HGQueryCondition topLevel;
    Map<String, Object> options;
    Map<HGQueryCondition, Set<HGQueryCondition>> transformed = new IdentityHashMap<HGQueryCondition, Set<HGQueryCondition>>();
    Map<HGQuery<?>, HGQueryCondition> translated = new IdentityHashMap<HGQuery<?>, HGQueryCondition>();
    Map<HGQuery<?>, QueryMetaData> metadata = new IdentityHashMap<HGQuery<?>, QueryMetaData>();
    
    Map<String, Set<HGQueryCondition>> redflags = new HashMap<String, Set<HGQueryCondition>>();
    
    HGQuery<SearchResult> query;

    // This is such a common operation when managing more complex structures..
    // maps of sets is common. And all the time you have to first check
    // if a set is already created for a given key and append to the set. 
    // With a method like this, you can do map.put(key, add(map.get(key), newelement));
    // in one line..
    //
    // maybe should put it in a global utils somewhere
    Set<HGQueryCondition> add(Set<HGQueryCondition> S, HGQueryCondition el)
    {
        if (S == null)
            S = new HashSet<HGQueryCondition>();
        S.add(el);
        return S;
    }
    Set<HGQueryCondition> add(Set<HGQueryCondition> S, Set<HGQueryCondition> els)
    {
        if (S == null)
            S = new HashSet<HGQueryCondition>();
        S.addAll(els);
        return S;
    }
    
    Set<HGQueryCondition> getSourceSet(HGQueryCondition c)
    {
        Set<HGQueryCondition> S = transformed.get(c);
        if (S == null)
        {
            S = new HashSet<HGQueryCondition>();
            transformed.put(c, S);
        }
        return S;
    }
    
    void collectSources(HGQueryCondition cond, Set<HGQueryCondition> sources)
    {
        Set<HGQueryCondition> S = transformed.get(cond);
        if (S != null)
        {
            for (HGQueryCondition x : S)
                collectSources(x, sources);
        }
        else
            sources.add(cond);            
    }
    
    Set<HGQueryCondition> findOrigin(HGQuery<?> q)
    {
        Set<HGQueryCondition> result = new HashSet<HGQueryCondition>();
        HGQueryCondition cond = translated.get(q);
        if (cond == null)
            return result;
        collectSources(cond, result);
        return result;
    }
    
    public void transformed(HGQueryCondition source, HGQueryCondition destination)
    {
        getSourceSet(destination).add(source);
    }
    
    public void transformed(Set<HGQueryCondition> source, HGQueryCondition destination)
    {
        getSourceSet(destination).addAll(source);
    }
    
    public void transformed(HGQueryCondition source, Set<HGQueryCondition> destination)
    {
        for (HGQueryCondition c : destination)
            getSourceSet(c).add(source);
    }
    
    public <R> void translated(HGQueryCondition source, HGQuery<R> destination, QueryMetaData metadata)
    {
        translated.put(destination, source);
        this.metadata.put(destination, metadata);
    }
    
    public AnalyzedQuery(HGQueryCondition topLevel, Map<String, Object> options)
    {
        this.topLevel = topLevel;
        this.options = options;
    }
    
    public Set<HGQueryCondition> getAnalysisResult(String option)
    {
        return redflags.get(option);
    }
    
    public HGSearchResult<SearchResult> execute()
    {
        return query.execute();                
    }
    
    
    public Map<String, Set<HGQueryCondition>> analyze()
    {
        redflags.clear();
        final Map<Class<?>, Mapping<Object, Boolean>> dispatch = 
                new HashMap<Class<?>, Mapping<Object, Boolean>>();
        if (options.containsKey(INTERSECTION_THRESHOLD))
            dispatch.put(IntersectionQuery.class, analyzeJoin);
        if (options.containsKey(SCAN_THRESHOLD))
            dispatch.put(PredicateBasedFilter.class, analyzePredicateFilter);
        HGUtils.visit(query, new Mapping<Object, Boolean>() {
           public Boolean eval(Object x)
           {
               if (x == null)
                   return Boolean.TRUE;
               Mapping<Object, Boolean> f = dispatch.get(x.getClass());
               if (f != null)
                   f.eval(x);
               return Boolean.TRUE;
           }
        });
        return redflags;
    }

    private long estimateSize(HGQuery<?> q)
    {
        QueryMetaData meta = metadata.get(q);
        if (meta != null)
            return meta.sizeUB;
        else
            return -1;
    }
    
    private Mapping<Object, Boolean> analyzeJoin = new Mapping<Object, Boolean>() {
        public Boolean eval(Object x)
        {
            IntersectionQuery<?> join = (IntersectionQuery<?>)x;
            long leftSize = estimateSize(join.getLeft());
            long rightSize = estimateSize(join.getRight());
            if (leftSize == -1)
                leftSize = rightSize;
            else if (rightSize == -1)
                rightSize = leftSize;
            if (Math.min(leftSize, rightSize) > (Integer)options.get(INTERSECTION_THRESHOLD))
            {
                And and = new And();
                and.addAll(findOrigin(join));
                redflags.put(INTERSECTION_THRESHOLD, add(redflags.get(INTERSECTION_THRESHOLD), and));
            }
            return Boolean.TRUE;
        }
    };    
    
    private Mapping<Object, Boolean> analyzePredicateFilter = new Mapping<Object, Boolean>() {
        public Boolean eval(Object x)
        {
            PredicateBasedFilter<?> pfilter = (PredicateBasedFilter<?>)x;
            long scanSize = estimateSize(pfilter.getQuery());
            if (scanSize > (Integer)options.get(SCAN_THRESHOLD))
            {
                Set<HGQueryCondition> S = findOrigin(pfilter.getQuery());
                assert S.size() == 1;
                redflags.put(SCAN_THRESHOLD, add(redflags.get(SCAN_THRESHOLD), S));
            }
            return Boolean.TRUE;
        }
    };
}