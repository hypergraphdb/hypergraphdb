package org.hypergraphdb.storage.incidence;

import java.util.ArrayList;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.IncidentCondition;
import org.hypergraphdb.query.MapCondition;
import org.hypergraphdb.query.Or;
import org.hypergraphdb.util.Mapping;

public class QueryByTypedIncident implements Mapping<HGQueryCondition, HGQueryCondition>
{
    private HyperGraph graph;
    
    public QueryByTypedIncident(HyperGraph graph)
    {
        this.graph = graph;
    }
    
    public HGQueryCondition eval(HGQueryCondition cond)
    {
        if (cond instanceof And)
        {
            And in = (And)cond;
            ArrayList<IncidentCondition> incident = new ArrayList<IncidentCondition>();
            And out = new And();
            
            HGHandle typeHandle = null;
            
            // Determine if there's a type constraint in this intersection
            for (HGQueryCondition sub : in)
            {
                if (sub instanceof AtomTypeCondition)
                {
                    AtomTypeCondition tsub = (AtomTypeCondition)sub;                   
                    if ( (typeHandle = tsub.typeHandleIfAvailable(graph)) == null)
                        out.add(sub); 
                }
                else if (sub instanceof IncidentCondition)
                    incident.add((IncidentCondition)sub);
                else
                    out.add(sub);
            }
            if (typeHandle != null)
            {
                for (IncidentCondition inc : incident)
                    out.add(new TypedIncidentCondition(inc.getTargetRef(), hg.constant(typeHandle)));
            }
            else
                out.addAll(incident);
            return out;
        }
        else if (cond instanceof Or)
        {
            Or in = (Or)cond;
            Or out = new Or();
            for (HGQueryCondition c : in)
                out.add(eval(c));
            return out;
        }
        else if (cond instanceof MapCondition)
        {
            MapCondition mcond = (MapCondition)cond;
            return new MapCondition(eval(mcond.getCondition()),
                                    mcond.getMapping());            
        }
        else
            return cond;
    }
}