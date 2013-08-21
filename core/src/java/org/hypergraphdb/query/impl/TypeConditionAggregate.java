package org.hypergraphdb.query.impl;

import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.TypeCondition;

public class TypeConditionAggregate implements HGQueryCondition
{
    public void watchTypeReference(TypeCondition tc)
    {
    }

    public boolean isEmpty()
    {
        return true;
    }
}
