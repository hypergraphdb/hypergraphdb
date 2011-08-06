package org.hypergraphdb;

import java.util.List;
import org.hypergraphdb.query.HGQueryCondition;

public interface HyperNode
{
    <T> T get(HGHandle handle);
    HGHandle add(Object atom, HGHandle type, int flags);
    public void define(HGHandle handle,
                       HGHandle type,
                       Object instance,
                       int flags);    
    boolean remove(HGHandle handle);
    boolean replace(HGHandle handle, Object newValue, HGHandle newType);
    HGHandle getType(HGHandle handle);
    IncidenceSet getIncidenceSet(HGHandle handle);
    
    <T> T findOne(HGQueryCondition condition);
    <T> HGSearchResult<T> find(HGQueryCondition condition);
    <T> T getOne(HGQueryCondition condition);
    <T> List<T> getAll(HGQueryCondition condition);
    <T> List<T> findAll(HGQueryCondition condition);
    long count(HGQueryCondition condition);
}