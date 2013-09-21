package org.hypergraphdb.query.cond2qry;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.ByTargetIndexer;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.AtomPartCondition;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.AtomValueCondition;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.IncidentCondition;
import org.hypergraphdb.query.IndexCondition;
import org.hypergraphdb.query.IndexedPartCondition;
import org.hypergraphdb.query.Nothing;
import org.hypergraphdb.query.OrderedLinkCondition;
import org.hypergraphdb.query.QueryCompile;
import org.hypergraphdb.query.TypedValueCondition;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Pair;
import org.hypergraphdb.util.Ref;

// Holds predefined contract transformations 
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ContractConjunction
{

    public static Pair<HGQueryCondition, Set<HGQueryCondition>> notransform = new Pair<HGQueryCondition, Set<HGQueryCondition>>(
            null, null);
    public static Pair<HGQueryCondition, Set<HGQueryCondition>> emptyresult = new Pair<HGQueryCondition, Set<HGQueryCondition>>(
            Nothing.Instance, new HashSet<HGQueryCondition>());

    public static class TypeValueContract implements QueryCompile.Contract
    {
        @Override
        public Pair<HGQueryCondition, Set<HGQueryCondition>> contract(HyperGraph graph,
                                                                      HGQueryCondition expression)
        {
            Map<Class<?>, Set<HGQueryCondition>> M = QEManip.find(
                    (Collection<HGQueryCondition>) expression,
                    AtomTypeCondition.class, AtomValueCondition.class,
                    TypedValueCondition.class);
            AtomTypeCondition tc = QEManip.collapse(graph, M
                    .get(AtomTypeCondition.class));
            AtomValueCondition vc = QEManip.collapse(graph, M
                    .get(AtomValueCondition.class));
            TypedValueCondition tvc = QEManip.collapse(graph, M
                    .get(TypedValueCondition.class));
            if (vc == null || tc == null)
                return notransform;
            if (tvc != null)
            {
                if (!tvc.getValueReference().equals(vc.getValueReference()))
                    throw new ContradictoryCondition();
            }
            else
                tvc = new TypedValueCondition(tc.getTypeReference(), vc
                        .getValueReference());
            return new Pair<HGQueryCondition, Set<HGQueryCondition>>(tvc,
                    (Set) HGUtils.set(vc));
        }
    }

    public static class ApplyByPartIndex implements QueryCompile.Contract
    {
        @Override
        public Pair<HGQueryCondition, Set<HGQueryCondition>> contract(HyperGraph graph,
                                                                      HGQueryCondition expression)
        {
            Map<Class<?>, Set<HGQueryCondition>> M = QEManip.find(
                    (Collection<HGQueryCondition>) expression,
                    AtomTypeCondition.class, AtomPartCondition.class);
            AtomTypeCondition bytype = QEManip.collapse(graph, M.get(AtomTypeCondition.class));
            if (bytype == null || hg.isVar(bytype.getTypeReference()))
                return notransform;
            Set<HGQueryCondition> bypart = M.get(AtomPartCondition.class);
            if (bypart == null)
                return notransform;
            HGHandle typeHandle = bytype.typeHandleIfAvailable(graph);
            if (typeHandle == null)
                return notransform;
            And out = new And();
            HGAtomType type = graph.get(typeHandle);
            Set<HGQueryCondition> replaced = new HashSet<HGQueryCondition>();
            for (HGQueryCondition q : bypart)
            {
                AtomPartCondition pc = (AtomPartCondition) q;
                if (TypeUtils.getProjection(graph, type, pc.getDimensionPath()) == null)
                    return emptyresult;
                else
                {
                    Pair<HGHandle, HGIndex<?,?>> p = ExpressionBasedQuery.findIndex(
                            graph, new ByPartIndexer(typeHandle, pc.getDimensionPath())); // graph.getIndexManager().getIndex(indexer);
                    if (p != null)
                    {
                        out.add(new IndexedPartCondition(p.getFirst(), p
                                .getSecond(), pc.getValueReference(), pc
                                .getOperator()));
                        replaced.add(q);
                        if (typeHandle.equals(p.getFirst()))
                            replaced.add(bytype);
                    }
                }
            }
            if (!out.isEmpty())
                return new Pair<HGQueryCondition, Set<HGQueryCondition>>(out,
                        replaced);
            else
                return notransform;
        }
    }

    public static class ApplyByTargetIndex implements QueryCompile.Contract
    {
        @Override
        public Pair<HGQueryCondition, Set<HGQueryCondition>> contract(HyperGraph graph,
                                                                      HGQueryCondition expression)
        {
            Map<Class<?>, Set<HGQueryCondition>> M = QEManip.find(
                    (Collection<HGQueryCondition>) expression,
                    AtomTypeCondition.class, OrderedLinkCondition.class);
            AtomTypeCondition bytype = QEManip.collapse(graph, M
                    .get(AtomTypeCondition.class));
            if (bytype == null || hg.isVar(bytype.getTypeReference()))
                return notransform;
            Set<HGQueryCondition> olinks = M.get(OrderedLinkCondition.class);
            if (olinks == null)
                return notransform;
            HGHandle typeHandle = bytype.typeHandleIfAvailable(graph);
            if (typeHandle == null)
                return notransform;
            And out = new And();
            Set<HGQueryCondition> replaced = new HashSet<HGQueryCondition>();
            for (HGQueryCondition q : olinks)
            {
                OrderedLinkCondition c = (OrderedLinkCondition) q;
                for (int ti = 0; ti < c.targets().length; ti++)
                {
                    Ref<HGHandle> targetHandle = c.targets()[ti];
                    if (hg.isVar(targetHandle)
                            || targetHandle.equals(hg.constant(graph
                                    .getHandleFactory().anyHandle())))
                        continue;
                    Pair<HGHandle, HGIndex<HGPersistentHandle,HGPersistentHandle>> p = ExpressionBasedQuery.findIndex(
                            graph, new ByTargetIndexer(typeHandle, ti));
                    if (p != null)
                    {
                        replaced.add(new IncidentCondition(targetHandle));
                        replaced.add(bytype);
                        out.add(new IndexCondition<HGPersistentHandle, HGPersistentHandle>(
                                p.getSecond(), targetHandle.get()
                                        .getPersistent()));
                    }
                }

            }
            if (!out.isEmpty())
                return new Pair<HGQueryCondition, Set<HGQueryCondition>>(out,
                        replaced);
            else
                return notransform;
        }
    }
}