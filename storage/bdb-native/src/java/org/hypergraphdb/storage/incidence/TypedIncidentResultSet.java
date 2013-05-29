package org.hypergraphdb.storage.incidence;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.query.impl.FilteredResultSet;

/**
 * <p>
 * This is a bit like the {@link FilteredResultSet}, but not quite since no external boolean function
 * is used, but rather the value's "annotation" content is evaluated in place. In addition, it 
 * supports the 'goTo' method so efficient intersections are still possible.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class TypedIncidentResultSet implements HGRandomAccessResult<HGHandle>
{
    private HGRandomAccessResult<HGHandle> irs = null;
    
    public TypedIncidentResultSet(HGRandomAccessResult<HGHandle> irs)
    {
        this.irs = irs;
    }

    public boolean hasPrev()
    {
        // TODO Auto-generated method stub
        return false;
    }

    public HGHandle prev()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean hasNext()
    {
        // TODO Auto-generated method stub
        return false;
    }

    public HGHandle next()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void remove()
    {
        // TODO Auto-generated method stub
        
    }

    public HGHandle current()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void close()
    {
        // TODO Auto-generated method stub
        
    }

    public boolean isOrdered()
    {
        // TODO Auto-generated method stub
        return false;
    }

    public org.hypergraphdb.HGRandomAccessResult.GotoResult goTo(HGHandle value,
                                                                 boolean exactMatch)
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void goAfterLast()
    {
        // TODO Auto-generated method stub
        
    }

    public void goBeforeFirst()
    {
        // TODO Auto-generated method stub
        
    }    
}
