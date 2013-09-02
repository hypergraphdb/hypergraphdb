package org.hypergraphdb.storage.incidence;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.IncidentCondition;
import org.hypergraphdb.query.QueryCompile;
import org.hypergraphdb.util.Pair;

public class QueryByTypedIncident implements QueryCompile.Contract
{
    public Pair<HGQueryCondition, Set<HGQueryCondition>> contract(HyperGraph graph,
                                                                  HGQueryCondition expression)
    {
        And in = (And) expression;
        HashSet<HGQueryCondition> replaced = new HashSet<HGQueryCondition>();
        ArrayList<IncidentCondition> incident = new ArrayList<IncidentCondition>();
        And out = new And();

        HGHandle typeHandle = null;
        AtomTypeCondition tsub = null;
        // Determine if there's a type constraint in this intersection
        for (HGQueryCondition sub : in)
        {
            if (sub instanceof AtomTypeCondition)
            {
                tsub = (AtomTypeCondition) sub;
                if ((typeHandle = tsub.typeHandleIfAvailable(graph)) != null)
                    replaced.add(sub);
            }
            else if (sub instanceof IncidentCondition)
                incident.add((IncidentCondition) sub);
        }
        if (typeHandle != null && !incident.isEmpty())
        {
            for (IncidentCondition inc : incident)
            {
                out.add(new TypedIncidentCondition(inc.getTargetRef(), hg
                        .constant(typeHandle)));
                replaced.add(inc);
            }
            return new Pair<HGQueryCondition, Set<HGQueryCondition>>(out,
                    replaced);
        }
        else
            return new Pair<HGQueryCondition, Set<HGQueryCondition>>(null, null);
    }
}