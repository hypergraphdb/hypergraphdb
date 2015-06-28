/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.cond2qry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.HGTransactionConfig;
import org.hypergraphdb.transaction.TransactionIsReadonlyException;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Pair;
import org.hypergraphdb.util.Ref;
import org.hypergraphdb.util.Var;
import org.hypergraphdb.util.VarContext;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.HGBreadthFirstTraversal;
import org.hypergraphdb.algorithms.HGTraversal;
import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.ByTargetIndexer;
import org.hypergraphdb.indexing.DirectValueIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.indexing.HGKeyIndexer;
import org.hypergraphdb.query.*;
import org.hypergraphdb.query.impl.AsyncSearchResult;
import org.hypergraphdb.query.impl.SyncSearchResult;
import org.hypergraphdb.query.impl.TypeConditionAggregate;

/**
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public class ExpressionBasedQuery<ResultType> extends HGQuery<ResultType>
{
	private HGQuery<ResultType> query = null; 
	private HGQueryCondition condition;
	private boolean hasVarContext = false;
	
	static <Key> Pair<HGHandle, HGIndex<Key, HGPersistentHandle>> findIndex(HyperGraph graph, HGKeyIndexer<Key> indexer)
	{
	    HGTraversal typeWalk = new HGBreadthFirstTraversal(indexer.getType(),
	                    new DefaultALGenerator(graph, 
	                                           hg.type(HGSubsumes.class), 
	                                           null, 
	                                           true, 
	                                           false, 
	                                           false));
	    for (HGHandle type = indexer.getType(); type != null; )
	    {
	        indexer.setType(type);
	        HGIndex<Key, HGPersistentHandle> idx = graph.getIndexManager().getIndex(indexer);
	        if (idx != null)
	            return new Pair<HGHandle, HGIndex<Key, HGPersistentHandle>>(type, idx);
	        else
	            type = typeWalk.hasNext() ? typeWalk.next().getSecond() : null;
	    }	    
	    return null;
	}

	private static void rememberInAnalyzer(HGQueryCondition src, HGQueryCondition dest)
	{
        AnalyzedQuery<?> aquery = (AnalyzedQuery<?>)VarContext.ctx().get("$analyzed").get();
        if (aquery != null && src != dest)
            aquery.transformed(src, dest);
	    
	}
	
	/**
	 * <p>Transform a query condition into a disjunctive normal form.</p>
	 * 
	 * @param C A condition, based on ands and ors to be transformed.
	 * @return The disjunctive normal form of C.
	 */
	private static HGQueryCondition toDNF(HGQueryCondition C)
	{
	    HGQueryCondition inputCondition = C;
		if (C instanceof And)
		{			
			And and = (And)C;
			HashSet<HGQueryCondition> andSet = new HashSet<HGQueryCondition>();			
			for (int i = 0; i < and.size(); i++)
			{
				HGQueryCondition sub = and.get(i);
				sub = toDNF(sub);
				// here 'sub' is either a primitive condition, a single 'And' or a
				// list of Or-ed Ands or primitives.
				if (sub instanceof And)
				{
					for (HGQueryCondition subsub:(And)sub)
						if (!andSet.contains(subsub))
							andSet.add(subsub);					
				}
				else if (sub instanceof Or)
				{
					Or result = new Or();
					for (HGQueryCondition subsub:(Or)sub)
					{
						And newsub = new And();
						newsub.add(subsub);
						newsub.addAll(andSet);
						newsub.addAll(and.subList(i + 1, and.size()));
						result.add(newsub);
					}
					C = toDNF(result);
					break;
				}				
				else
					andSet.add(sub);
			}
			and = new And();
			and.addAll(andSet);
			if (C == inputCondition)
			    C = and;
		}
		else if (C instanceof Or)
		{
			Or or = (Or)C;
			HashSet<HGQueryCondition> orSet = new HashSet<HGQueryCondition>();
			for (int i = 0; i < or.size(); i++)
			{
				HGQueryCondition sub = or.get(i);
				sub = toDNF(sub);
				if (sub instanceof Or)
				{					
					for (HGQueryCondition subsub:(Or)sub)
						if (!orSet.contains(subsub))
							orSet.add(subsub);
				}
				else if (!orSet.contains(sub))
					orSet.add(sub);
			}
			or = new Or();
			or.addAll(orSet);
			C = or;
		}
        else if (C instanceof MapCondition)
        {
            MapCondition mcond = (MapCondition)C;
            C = new MapCondition(toDNF(mcond.getCondition()), mcond.getMapping());
        }		

        AnalyzedQuery<?> aquery = (AnalyzedQuery<?>)VarContext.ctx().get("$analyzed").get();
        if (aquery != null && inputCondition != C)
            aquery.transformed(inputCondition, C);
        rememberInAnalyzer(inputCondition, C);
        return C;
	}
	
	// Condition use the same type either if both references are constant or both
	// refer to the same variable
	private boolean isSameType(TypeCondition c1, TypeCondition c2)
	{
		Ref<?> r1 = c1.getTypeReference(), r2 = c2.getTypeReference();
		return !hg.isVar(r1) && !hg.isVar(r2) && r1.get().equals(r2.get()) ||
			    hg.isVar(r1) && hg.isVar(r2) && this.ctx.isSameVar((Var<?>)r1, (Var<?>)r2);	
	}
	
	private boolean checkConsistent(AtomTypeCondition c1, AtomTypeCondition c2)
	{
		if (c1.getTypeReference().get() == null || c2.getTypeReference() == null ||
			hg.isVar(c1.getTypeReference()) || hg.isVar(c2.getTypeReference()))
			return true;
		HGHandle h1 = (c1.getTypeHandle() != null) ? 
						c1.getTypeHandle() : 
						graph.getTypeSystem().getTypeHandle(c1.getJavaClass());
		if (c2.getTypeHandle() != null)
			return h1.equals(c2.getTypeHandle());
		else
			return h1.equals(graph.getTypeSystem().getTypeHandle(c2.getJavaClass()));
	}
	
	private boolean checkConsistent(TypedValueCondition c1, AtomTypeCondition c2)
	{
		if (c2.getTypeReference() == null || c1.getTypeReference() == null ||
				hg.isVar(c1.getTypeReference()) || hg.isVar(c2.getTypeReference()))
			return true;
		HGHandle h1 = (c1.getTypeHandle() != null) ? 
				c1.getTypeHandle() : 
				graph.getTypeSystem().getTypeHandle(c1.getJavaClass());
		if (c2.getTypeHandle() != null)
			return h1.equals(c2.getTypeHandle());
		else
			return h1.equals(graph.getTypeSystem().getTypeHandle(c2.getJavaClass()));		
	}
	
	private boolean checkConsistent(TypedValueCondition tc, AtomValueCondition vc)
	{
		if (tc.getValueReference() == null || vc.getValueReference() == null ||
			hg.isVar(tc.getValueReference()) || hg.isVar(vc.getValueReference()))
			return true;
		return HGUtils.eq(tc.getValue(), vc.getValue()) && tc.getOperator() == vc.getOperator();
	}

	private boolean checkConsistent(HGAtomType type, AtomPartCondition vc)
	{
		return TypeUtils.getProjection(graph, type, vc.getDimensionPath()) != null;
	}
	
    private HGQueryCondition simplify(HGQueryCondition cond)
    {
        //return oldsimplify(cond);
        return newsimplify(cond);
    }
    
    @SuppressWarnings("rawtypes")
    private HGQueryCondition newsimplify(HGQueryCondition cond)
    {
        if (! (cond instanceof And))
            return oldsimplify(cond);
        And in = new And();
        in.addAll((And)cond);
        Map<Class<?>, Set<HGQueryCondition>> allbytype =  QEManip.find(in, TypeCondition.class);
        if (!allbytype.isEmpty())
        {
            Set<TypeCondition> S = (Set)allbytype.get(TypeCondition.class);
            Pair<AtomTypeCondition, TypeConditionAggregate> p = QEManip.reduce(graph, S);
            if (p != null)
            {
                // We replace all "pure" AtomTypeCondition by the one returned
                // from the reduction. 
                for (TypeCondition tc : S)
                    if (tc instanceof AtomTypeCondition)
                        in.remove(tc);
                in.add(p.getFirst());
                if (!p.getSecond().isEmpty())
                    in.add(p.getSecond());
            }
        }
        
        And out = new And();
        List<QueryCompile.Contract> L = graph.getConfig().getQueryConfiguration().getContractTransforms(And.class);
        HashSet<HGQueryCondition> ignore = new HashSet<HGQueryCondition>();
        HashSet<HGQueryCondition> added = new HashSet<HGQueryCondition>();
        try
        {
            for (QueryCompile.Contract contract : L)            
            {
                Pair<HGQueryCondition, Set<HGQueryCondition>> p = contract.contract(graph, in);
                if (p.getFirst() == null) continue;
                else if (p.getFirst() == Nothing.Instance)
                    return Nothing.Instance;
                ignore.addAll(p.getSecond());
                added.add(p.getFirst());
            }
        }
        catch (ContradictoryCondition ccex)
        {
            return Nothing.Instance;
        }
        for (HGQueryCondition c : in) if (!ignore.contains(c))  out.add(c);
        for (HGQueryCondition c : added) if (!ignore.contains(c))   out.add(c);
        return out;
    }
    
    
    // apply a few simply transformation for common cases...
    // this kind of assumes that we are already in DNF because
    // conjunction are handled by transforming them as if we are at
    // the top level
    private HGQueryCondition oldsimplify(HGQueryCondition cond)
    {
        if (cond instanceof And)
        {
            And in = (And)cond;
            And out = new And();
            for (HGQueryCondition c : in)
            {
                rememberInAnalyzer(c, c = simplify(c));
                if (c instanceof And)
                    out.addAll((And)c);
                else if (c == Nothing.Instance)
                    return c;
                else
                    out.add(c);
            }
            
            // At the end of the following step, the conjunction will have
            // either a single TypedValueCondition or at most one AtomTypeCondition
            // and one AtomValueCondition. This step insures that there are no
            // contradictory conditions amongst the condition of type
            // AtomValueCondition, AtomTypeCondition and TypedValueCondition
            AtomTypeCondition byType = null;
            AtomValueCondition byValue = null;
            TypedValueCondition byTypedValue = null;
            HashSet<OrderedLinkCondition> oLinks = new HashSet<OrderedLinkCondition>();
            HashSet<AtomPartCondition> byPart = new HashSet<AtomPartCondition>();
            boolean has_ordered = false;
            boolean has_ra = false;
            for (Iterator<HGQueryCondition> i = out.iterator(); i.hasNext(); )
            {
                HGQueryCondition c = i.next();
                if (c instanceof AtomTypeCondition)
                {
                    AtomTypeCondition tc = (AtomTypeCondition)c;                    
                    // if the condition is on a Java class with no HG type currently defined,
                    // clearly there can't be atoms of that type
//                      if (!hg.isVar(tc.getTypeReference()) && 
//                          tc.getTypeHandle() == null && 
//                          graph.getTypeSystem().getTypeHandleIfDefined(tc.getJavaClass()) == null)
//                          
//                          return Nothing.Instance;
                        
                    if (byType == null)
                    {
                        if (byTypedValue != null)
                        {
                            if(!checkConsistent(byTypedValue, tc))
                                return Nothing.Instance;
                            else if (isSameType(byTypedValue, tc))
                                i.remove();
                        }
                        else
                            byType = tc;
                    }
                    else if (isSameType(byType, tc))
                        i.remove();
                    else if (!checkConsistent(byType, tc))
                        return Nothing.Instance;                            
                }
                else if (c instanceof AtomValueCondition)
                {
                    if (byValue == null)
                    {                       
                        if (byTypedValue != null)
                            if(!checkConsistent(byTypedValue, (AtomValueCondition)c))
                                return Nothing.Instance;
                            else
                                i.remove();
                        else
                            byValue = (AtomValueCondition)c;                        
                    }
                    else if (byValue.equals(c))
                        i.remove();
                    else
                        return Nothing.Instance;
                }
                else if (c instanceof TypedValueCondition)
                {
                    if (byTypedValue ==  null)
                        byTypedValue = (TypedValueCondition)c;
                    else if (byTypedValue.equals(c) &&                  
                            isSameType(byTypedValue, (TypedValueCondition)c))
                        i.remove();
                    else
                        return Nothing.Instance;            
                    // if the condition is on a Java class with no HG type currently defined,
                    // clearly there can't be atoms of that type
                    if (!hg.isVar(byTypedValue.getTypeReference()) && 
                            byTypedValue.getTypeHandle() == null && 
                        graph.getTypeSystem().getTypeHandleIfDefined(byTypedValue.getJavaClass()) == null)                      
                        return Nothing.Instance;
                }
                else if (c instanceof AtomPartCondition)
                {
                    byPart.add((AtomPartCondition)c);
                }
                else if (c instanceof OrderedLinkCondition)
                {
                    oLinks.add((OrderedLinkCondition)c);
                }
                else
                {
                    ConditionToQuery<?> transform = QueryCompile.translator(graph, c.getClass());
                    if (transform != null)
                    {
                        QueryMetaData qmd = transform.getMetaData(graph, c);
                        has_ordered = has_ordered || qmd.ordered;
                        has_ra = has_ra || qmd.randomAccess;
                    }
                }
            }
            
            HGHandle typeHandle  = null;
            
            if (byTypedValue != null)
            {
                if (byType != null)
                {
                    if (!checkConsistent(byTypedValue, byType))
                        return Nothing.Instance;
                    else if (isSameType(byTypedValue, byType))
                    {
                        out.remove(byType);
                        if (byType.getTypeHandle() != null && !hg.isVar(byType.getTypeReference()))
                            typeHandle = byType.getTypeHandle();
                        byType = null;
                    }
                }
                if (byValue != null)
                    if (!checkConsistent(byTypedValue, byValue))
                        return Nothing.Instance;
                    else
                    {
                        out.remove(byValue);
                        byValue = null;
                    }               
                if (typeHandle == null && !hg.isVar(byTypedValue.getTypeReference()))
                    if (byTypedValue.getTypeHandle() != null)
                        typeHandle = byTypedValue.getTypeHandle();
                    else
                        typeHandle = graph.getTypeSystem().getTypeHandle(byTypedValue.getJavaClass());
            }
            else if (byType != null)
            {
                if (!hg.isVar(byType.getTypeReference()))
                {
                    if (byType.getTypeHandle() != null)
                        typeHandle  = byType.getTypeHandle();
                    else
                        typeHandle = graph.getTypeSystem().getTypeHandleIfDefined(byType.getJavaClass());
                    if (typeHandle == null)
                        return Nothing.Instance;
                }
                if (byValue != null)
                {
                    out.add(byTypedValue = new TypedValueCondition(byType.getTypeReference(), 
                                                                   byValue.getValueReference(), 
                                                                   byValue.getOperator()));
                    out.remove(byType);
                    out.remove(byValue);
                    byType = null;
                    byValue = null;
                }
            }
            
            // now, we check for availabe indices
            
            // The simplest: a direct by-value index 
            if (byTypedValue != null && 
                !hg.isVar(byTypedValue.getTypeReference()))
            {
                Pair<HGHandle, HGIndex<Object,HGPersistentHandle>> p = findIndex(graph, new DirectValueIndexer<Object>(typeHandle));
                if (p != null)
                {
                    out.remove(byTypedValue);
                    out.add(new IndexCondition(p.getSecond(), 
                                               byTypedValue.getValueReference(),
                                               ComparisonOperator.EQ));
                }
            }
            
            // indexing by value parts, if we find an appropriate index
            // then we can eliminate the "type" predicate altogether (since bypart indices
            // are always for a particular atom type) and a "by value" condition is kept
            // only as a predicate
            if (typeHandle != null && byPart.size() > 0)
            {
                HGAtomType type = (HGAtomType)graph.get(typeHandle);
                if (type == null)
                    throw new HGException("No type for type handle " + typeHandle + " in this HyperGraph instance.");
                for (AtomPartCondition pc : byPart)                                 
                    if (!checkConsistent(type, pc))
                        return Nothing.Instance;
                    else
                    {
                        Pair<HGHandle, HGIndex<?,?>> p = findIndex(graph, new ByPartIndexer(typeHandle, 
                                                        pc.getDimensionPath())); //graph.getIndexManager().getIndex(indexer);
                        if (p != null)
                        {
                            if (typeHandle.equals(p.getFirst()))
                            {
                                if (byType != null)
                                {
                                    out.remove(byType);
                                    byType = null;
                                }
                                else if (byTypedValue != null)
                                {
                                    out.remove(byTypedValue);
                                    out.add(new ValueAsPredicateOnly(byTypedValue.getValueReference(), 
                                                                     byTypedValue.getOperator()));
                                    byTypedValue = null;
                                }
                            }
                            out.remove(pc);                         
                            out.add(new IndexedPartCondition(p.getFirst(), 
                                                             p.getSecond(), 
                                                             pc.getValueReference(), 
                                                             pc.getOperator()));
                        }
                    }
            }
            
            // Check for "by-target" indices within an OrderedLinkConditions and replace
            // the corresponding 'incident' condition with one based on the index.
            // Here would be an opportunity to use HGTypeStructuralInfo on a link type and
            // possibly eliminate the OrderedLinkCondition (and resulting predicate call during
            // query execution) altogether
            if (typeHandle != null)
                for (OrderedLinkCondition c : oLinks)
                {                   
                    for (int ti = 0; ti < c.targets().length; ti++)
                    {                   
                        Ref<HGHandle> targetHandle = c.targets()[ti];
                        if (hg.isVar(targetHandle) || targetHandle.equals(hg.constant(graph.getHandleFactory().anyHandle())))
                            continue;
                        Pair<HGHandle, HGIndex<HGPersistentHandle,HGPersistentHandle>> p = 
                                findIndex(graph, new ByTargetIndexer(typeHandle, ti));
                        if (p != null)
                        {
                            if (typeHandle.equals(p.getFirst()))
                            {
                                if (byType != null)
                                {
                                    out.remove(byType);
                                    byType = null;
                                }
                                else if (byTypedValue != null)
                                {
                                    out.remove(byTypedValue);
                                    out.add(new AtomValueCondition(byTypedValue.getValueReference(), 
                                                                   byTypedValue.getOperator()));
                                    byTypedValue = null;
                                }                           
                            }
                            out.remove(new IncidentCondition(targetHandle));                            
                            out.add(new IndexCondition<HGPersistentHandle, HGPersistentHandle>(
                                        p.getSecond(), targetHandle.get().getPersistent()));
                        }
                    }
                }
            return out.size() > 1 ? out : out.iterator().next();
        }
        else if (cond instanceof Or)
        {
            Or in = (Or)cond;
            if (in.isEmpty())
                return Nothing.Instance;
            Or out = new Or();
            for (HGQueryCondition c : in)
            {               
                rememberInAnalyzer(c, c = simplify(c));
                if (c instanceof Or)
                    out.addAll((Or)c);              
                else if (c != Nothing.Instance)
                    out.add(c);
            }           
            return out.isEmpty() ? Nothing.Instance : 
                    out.size() > 1 ? out : out.iterator().next();
        }
        else if (cond instanceof MapCondition)
        {
            MapCondition mcond = (MapCondition)cond;
            HGQueryCondition mapped = simplify(mcond.getCondition());
            rememberInAnalyzer(mcond.getCondition(), mapped);
            return new MapCondition(mapped, mcond.getMapping());
        }
        else
            return cond;
    }   

	private List<AtomPartCondition> getAtomIndexedPartsConditions(HyperGraph graph, HGHandle hType, Object value)
	{
		ArrayList<AtomPartCondition> L = new ArrayList<AtomPartCondition>();
		List<HGIndexer<?,?>> indexers = graph.getIndexManager().getIndexersForType(hType);
		if (indexers == null)
			return L;
		for (HGIndexer<?,?> idx : indexers)
		{
			if (idx instanceof ByPartIndexer)
			{
				String [] dimPath = ((ByPartIndexer)idx).getDimensionPath();
				Object partValue = TypeUtils.project(graph, hType, value, dimPath, true).getValue();
				L.add(new AtomPartCondition(dimPath, partValue));
			}
		}
		return L;
	}
	
	private HGHandle classToHandle(Class<?> cl)
	{
		HGHandle h = graph.getTypeSystem().getTypeHandleIfDefined(cl);
		if (h == null)
		{
			 HGTransaction tx = graph.getTransactionManager().getContext().getCurrent();
			 if (tx == null || !tx.isReadOnly())
				 h = graph.getTypeSystem().getTypeHandleIfDefined(cl);
		}
		return h;
	}
	
	private HGQueryCondition expand(HyperGraph graph, HGQueryCondition cond)
	{
	    HGQueryCondition inputCondition = cond;
		if (cond instanceof TypePlusCondition)
		{
			TypePlusCondition ac = (TypePlusCondition)cond;
			if (ac.getBaseType() == null)
				ac.setBaseType(classToHandle(ac.getJavaClass()));
//			{
//				HGHandle typeHandle = graph.getTypeSystem().getTypeHandleIfDefined(ac.getJavaClass());
//				if (typeHandle != null)
//					ac.setBaseType(typeHandle);
//				else 
//					cond = Nothing.Instance;
//			}
			if (ac.getBaseType() != null)
			{
				Or orCondition = new Or();
	            for (HGHandle h : ac.getSubTypes(graph))
	            	orCondition.add(new AtomTypeCondition(h));
	            cond = orCondition;
			}
			else
				cond = Nothing.Instance;
		}
//		else if (cond instanceof AtomTypeCondition)
//		{
//			AtomTypeCondition tc = (AtomTypeCondition)cond;
//			if (tc.getJavaClass() != null && 
//				!hg.isVar(tc.getTypeReference()) && 
//				graph.getTypeSystem().getTypeHandleIfDefined(tc.getJavaClass()) == null)
//				
//					cond = Nothing.Instance;
//		}
		else if (cond instanceof TypedValueCondition)
		{
			TypedValueCondition tc = (TypedValueCondition)cond;
			if (!hg.isVar(tc.getTypeReference()))
			{
                Pair<HGHandle, HGIndex<Object, HGPersistentHandle>> p = findIndex(graph, new DirectValueIndexer<Object>(tc.getTypeHandle()));
                if (p != null)
                    cond = new IndexCondition(p.getSecond(), tc.getValueReference(), tc.getOperator());
                else if (((TypedValueCondition)cond).getOperator() == ComparisonOperator.EQ)
                {
    				HGHandle typeHandle = tc.getTypeHandle();
    				if (typeHandle == null)
    					typeHandle = classToHandle(tc.getJavaClass());
    				if (!hg.isVar(tc.getValueReference()))
    				{
    					List<AtomPartCondition> indexedParts = getAtomIndexedPartsConditions(graph, typeHandle, tc.getValue());
    					if (!indexedParts.isEmpty())
    					{
    						And and = hg.and(cond);
    						for (AtomPartCondition pc : indexedParts)
    							and.add(pc);
    						cond = and;
    					}
    				}
                }
			}
		}
		else if (cond instanceof AtomValueCondition && 
				 !hg.isVar(((AtomValueCondition)cond).getValueReference()))
		{
			AtomValueCondition vc = (AtomValueCondition)cond;
            Object value = vc.getValue();
            if (value == null)
                throw new HGException("Search by null values is not supported yet.");
            HGHandle valueHandle = graph.getHandle(value);
            HGHandle type = null;
            if (valueHandle != null)
            	type = graph.getTypeSystem().getTypeHandle(valueHandle);
            else if (value != null)
            	type = graph.getTypeSystem().getTypeHandleIfDefined(value.getClass());            
            if (type != null && vc.getOperator() == ComparisonOperator.EQ)
            {
    			List<AtomPartCondition> indexedParts = getAtomIndexedPartsConditions(graph, type, value);
    			if (!indexedParts.isEmpty())
    			{
    				And and = hg.and(cond, new AtomTypeCondition(type));
    				for (AtomPartCondition pc : indexedParts)
    					and.add(pc);
    				cond = and;
    			}
            }
		}
		else if (cond instanceof And)
		{
			HGQueryCondition statedType = null;
			And result = new And();
			for (HGQueryCondition sub : (And)cond)
			{
				if (sub instanceof AtomTypeCondition || sub instanceof TypedValueCondition)
					statedType = sub;
				HGQueryCondition expanded = expand(graph, sub);
				if (expanded == Nothing.Instance)
					cond = Nothing.Instance;
				else
				{
					if (expanded instanceof And)
						result.addAll((And)expanded);
					else
						result.add(expanded);
				}
			}
			if (statedType != null) // filter out any (possibly wrongly) inferred type conditions during the expansion process
				for (Iterator<HGQueryCondition> i = result.iterator(); i.hasNext(); )
				{
					HGQueryCondition curr = i.next();
					if (! (curr instanceof AtomTypeCondition)) continue;
					if (!isSameType((TypeCondition)curr, (TypeCondition)statedType))
						i.remove();
				}
			if (cond != Nothing.Instance)
				cond = result;
		}
		else if (cond instanceof Or)
		{
			Or result = new Or();
			for (HGQueryCondition sub : (Or)cond)
			{
				HGQueryCondition subexp = expand(graph, sub);
				if (subexp != Nothing.Instance)
					result.add(subexp);
			}			
			cond = result.isEmpty() ? Nothing.Instance : result;			
		}
		else if (cond instanceof OrderedLinkCondition)
		{
			And result = new And();
			result.add(cond);
			for (Ref<HGHandle> h : ((OrderedLinkCondition)cond).targets())
				if (!hg.isVar(h) && !h.get().equals(graph.getHandleFactory().anyHandle()))
					result.add(new IncidentCondition(h));
			cond = result;
		}
		else if (cond instanceof LinkCondition)
		{
			And result = new And();
			for (Ref<HGHandle> h : ((LinkCondition)cond).targets())
				if (!hg.isVar(h) && !h.get().equals(graph.getHandleFactory().anyHandle()))
					result.add(new IncidentCondition(h));
			cond = result;
		}		
		else if (cond instanceof MapCondition)
		{
            MapCondition mcond = (MapCondition)cond;
            cond = new MapCondition(expand(graph, mcond.getCondition()),
                                    mcond.getMapping());		    
		}
		rememberInAnalyzer(inputCondition, cond);		
		return cond;
	}
	
	/**
	 * <p>
	 * Recursively replace the static hg.anyHandle by the handle factory
	 * anyHandle of the current graph. 
	 * </p>
	 * @param c
	 */
	private void preprocess(HGQueryCondition c)
	{
	    if (c instanceof LinkCondition)
	    {
	        LinkCondition lc = (LinkCondition)c;
	        if (lc.getTargetSet().contains(hg.constant(hg.anyHandle())))
	        {
	            lc.getTargetSet().remove(hg.constant(hg.anyHandle()));
	            lc.getTargetSet().add(hg.constant((HGHandle)graph.getHandleFactory().anyHandle()));
	        }
	    }
	    else if (c instanceof OrderedLinkCondition)
	    {
	        OrderedLinkCondition lc = (OrderedLinkCondition)c;
	        for (int i = 0; i < lc.getTargets().length; i++)
	            if (lc.getTargets()[i].equals(hg.constant(hg.anyHandle())))
	                lc.getTargets()[i] = hg.constant((HGHandle)graph.getHandleFactory().anyHandle());
	    }
	    else if (c instanceof Not)
	    {
	        if (((Not) c).getPredicate() instanceof HGQueryCondition)
	        preprocess((HGQueryCondition)((Not)c).getPredicate());
	    }
	    else if (c instanceof And)
	    {
	        for (HGQueryCondition sub : (And)c)
	            preprocess(sub);
	    }
        else if (c instanceof Or)
        {
            for (HGQueryCondition sub : (Or)c)
                preprocess(sub);
        }	    
        else if (c instanceof MapCondition)
        {
            preprocess(((MapCondition)c).getCondition());
        }
	}
	
	public ExpressionBasedQuery(final HyperGraph graph, boolean withVarContext)
	{
		this.graph = graph;
		if (this.hasVarContext = withVarContext)
			this.ctx = VarContext.pushFrame();		
	}
	
	public ExpressionBasedQuery(final HyperGraph graph, final HGQueryCondition condition)
	{
		this.graph = graph;
		graph.getTransactionManager().ensureTransaction(new Callable<HGQuery<ResultType>>() {
			public HGQuery<ResultType> call()
			{
				return compileProcess(condition);
			}
		}, HGTransactionConfig.READONLY);
	}
	
	public HGQuery<ResultType> compile(final HGQueryCondition condition)
	{
	    try
	    {
    		return graph.getTransactionManager().ensureTransaction(new Callable<HGQuery<ResultType>>() {
    			public HGQuery<ResultType> call()
    			{
    				return compileProcess(condition);
    			}
    		}, HGTransactionConfig.READONLY);
	    }
	    catch (Throwable t)
	    {
	        if (HGUtils.getRootCause(t) instanceof TransactionIsReadonlyException)
	        {
	            if (this.hasVarContext) // it would have been popped when the transaction was aborted, so we push a new one
	                VarContext.pushFrame(this.ctx);      	            
	            return graph.getTransactionManager().ensureTransaction(new Callable<HGQuery<ResultType>>() {
	                public HGQuery<ResultType> call()
	                {
	                    return compileProcess(condition);
	                }});
	        }
	        else if (t instanceof RuntimeException)
	            throw (RuntimeException)t;
	        else if (t instanceof Error)
	            throw (Error)t;
	        else 
	            throw new RuntimeException(t);
	    }
	}
	
	private HGQuery<ResultType> compileProcess(final HGQueryCondition condition)
	{
		// The condition was constructed before the make method is called and all variables have been
		// added to the current top-level context. The make method takes ownership of that context
		// and removes from the stack at the end. The correct way to use variables in query
		// conditions is calling the varContext() method before calling HGQuery.make
		try
		{
            QueryCompile.start();
            preprocess(condition);  
            this.condition = simplify(toDNF(expand(graph, condition)));
            rememberInAnalyzer(condition, this.condition);          
            query = QueryCompile.translate(graph, this.condition);      
            return this;
		}
		finally
		{
			// Cleanup thread-bound variable context
			if (hasVarContext)
				VarContext.popFrame();
			QueryCompile.finish();
		}
	}
	
    public HGSearchResult<ResultType> execute()
    {    	
        HGSearchResult<ResultType> rs = query.execute();
        if (rs instanceof AsyncSearchResult)
            return new SyncSearchResult<ResultType>((AsyncSearchResult<ResultType>)rs);
        else
            return rs;
    }
    
    /** 
     * <p>Return a possibly simplified and normalized version of the condition with
     * which this query was constructed.</p>
     */
    public HGQueryCondition getCondition()
    {
    	return condition;
    }
    
  public HGQuery<ResultType> getCompiledQuery()
    {
        return query;
    }
}