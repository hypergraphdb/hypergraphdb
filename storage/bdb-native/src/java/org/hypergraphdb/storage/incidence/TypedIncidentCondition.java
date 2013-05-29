package org.hypergraphdb.storage.incidence;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.util.Ref;

public class TypedIncidentCondition implements HGQueryCondition
{
    private Ref<HGHandle> targetRef;
    private Ref<HGHandle> typeRef;
    
    public TypedIncidentCondition() { }
    public TypedIncidentCondition(Ref<HGHandle> target, Ref<HGHandle> typeRef)
    {
        this.targetRef = target;
        this.typeRef = typeRef;
    }
    
    public Ref<HGHandle> getTargetRef()
    {
        return targetRef;
    }
    public void setTargetRef(Ref<HGHandle> targetRef)
    {
        this.targetRef = targetRef;
    }
    public Ref<HGHandle> getTypeRef()
    {
        return typeRef;
    }
    public void setTypeRef(Ref<HGHandle> typeRef)
    {
        this.typeRef = typeRef;
    }    
}