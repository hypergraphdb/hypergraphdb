package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.util.Mapping;

public class FilteredRAResultSet<T> extends FilteredResultSet<T> implements HGRandomAccessResult<T>
{
    private HGRandomAccessResult<T> rs;
    
    public FilteredRAResultSet(HGRandomAccessResult<T> searchResult,
                               Mapping<T, Boolean> predicate, 
                               int lookahead)
    {
        super(searchResult, predicate, lookahead);
        rs = searchResult;
    }

    
    public HGRandomAccessResult.GotoResult goTo(T value, boolean exactMatch)
    {
        GotoResult r = rs.goTo(value, exactMatch);
        if (exactMatch)
            return r == GotoResult.found && predicate.eval(value) ? GotoResult.found : GotoResult.nothing;
        if (predicate.eval(value))
            return r;
        while (rs.hasNext())
            if (predicate.eval(rs.next()))
                return GotoResult.close;
        return GotoResult.nothing;
    }
    
    public void goAfterLast()
    {
        rs.goAfterLast();
        while (rs.hasPrev() && !predicate.eval(rs.prev()));
        if (!rs.hasNext())
            rs.goAfterLast();
        else
            rs.next();        
    }
    
    public void goBeforeFirst()
    {
        rs.goBeforeFirst();
        while (rs.hasNext() && !predicate.eval(rs.next()));
        if (!rs.hasPrev())
            rs.goBeforeFirst();
        else
            rs.prev();
    }   
}