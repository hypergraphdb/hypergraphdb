package org.hypergraphdb.query.cond2qry;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.HGTraversal;
import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.TypeCondition;
import org.hypergraphdb.query.impl.TypeConditionAggregate;
import org.hypergraphdb.util.Pair;
import org.hypergraphdb.util.Ref;

public class QEManip
{

    // Compare left and right and return the one which is sub-type (descended in
    // a subsumes chain) of the
    // other. If the two are not related through a sub-typing relationship,
    // return null.
    public static HGHandle findMostSpecific(HyperGraph graph, HGHandle left,
                                            HGHandle right)
    {
        if (left.equals(right))
            return left;
        // First we check if right is a sub-type of left
        HGTraversal trav = hg.bfs(left, hg.type(HGSubsumes.class), null, false,
                true).getTraversal(graph);
        while (trav.hasNext())
            if (trav.next().equals(right))
                return right;
        trav = hg.bfs(right, hg.type(HGSubsumes.class), null, false, true)
                .getTraversal(graph);
        while (trav.hasNext())
            if (trav.next().equals(left))
                return left;
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <T extends HGQueryCondition> T collapse(HyperGraph graph,
                                                          Set<HGQueryCondition> S)
    {
        if (S == null || S.isEmpty())
            return null;
        Iterator<HGQueryCondition> I = S.iterator();
        HGQueryCondition c = I.next();
        while (I.hasNext())
            if (!c.equals(I.next()))
                throw new ContradictoryCondition();
        return (T) c;
    }

    /**
     * Return the one true type condition out of a conjunction of multiple
     * conditions that constrain by type in one way or another. The method
     * returns null if no type is actually specified already (this means all
     * type conditions in the passed in set are using variables). If there is an
     * inconsistency b/w two type conditions so that the result set should be
     * really empty, the method returns and AtomTypeCondition with a null
     * reference (i.e. "no type possible").
     * 
     * @param graph
     * @param bytype
     * @return
     */
    public static Pair<AtomTypeCondition, TypeConditionAggregate> reduce(HyperGraph graph,
                                                                         Set<TypeCondition> bytype)
    {
        HGHandle typeHandle = null;
        TypeConditionAggregate taggr = new TypeConditionAggregate();
        for (TypeCondition c : bytype)
        {
            if (hg.isVar(c.getTypeReference()))
            {
                taggr.watchTypeReference(c);// ((Var<?>)c.getTypeReference());
                continue;
            }
            HGHandle th = null;
            if (c.getTypeReference().get() instanceof Class<?>)
            {
                th = graph.getTypeSystem().getTypeHandleIfDefined(
                        c.getTypeReference().getClass());
                if (th == null)
                {
                    taggr.watchTypeReference(c);
                    continue;
                }
            }
            else
                th = (HGHandle) c.getTypeReference().get();
            if (typeHandle != null)
            {
                typeHandle = findMostSpecific(graph, typeHandle, th);
                if (typeHandle == null) // are they incompatible?
                    return new Pair<AtomTypeCondition, TypeConditionAggregate>(
                            new AtomTypeCondition((Ref<?>) null), taggr);
            }
            else
                typeHandle = th;
        }
        // At this point we have possibly found one most specific type handle
        // specified as part of the
        // conjunction and possibly several conditions with variable type or
        // unresolved Java class (which
        // for all intents and purposes are like variables right now.
        if (typeHandle != null)
            return new Pair<AtomTypeCondition, TypeConditionAggregate>(
                    new AtomTypeCondition(hg.constant(typeHandle)), taggr);
        else
            return null;
    }

    /**
     * <p>
     * Collect and group different types of conditions in map. The method takes
     * any number of condition Class-es and return a map Class->Set<all
     * conditions of that class>. The test is done using the
     * <code>Class.isAssignableFrom</code> so a given condition could end in
     * multiple map entries if some of the Class arguments inherit from each
     * other. If no condition of a specified Class is found, the map won't
     * contain an entry for that class at all.
     * </p>
     * 
     * @param C
     * @param condType
     * @return
     */
    public static Map<Class<?>, Set<HGQueryCondition>> find(Collection<HGQueryCondition> C,
                                                            Class<?>... condType)
    {
        HashMap<Class<?>, Set<HGQueryCondition>> M = new HashMap<Class<?>, Set<HGQueryCondition>>();
        for (HGQueryCondition c : C)
        {
            for (int i = 0; i < condType.length; i++)
                if (condType[i].isAssignableFrom(c.getClass()))
                {
                    Set<HGQueryCondition> S = M.get(condType[i]);
                    if (S == null)
                    {
                        S = new HashSet<HGQueryCondition>();
                        M.put(condType[i], S);
                    }
                    S.add(c);
                }
        }
        return M;
    }
}