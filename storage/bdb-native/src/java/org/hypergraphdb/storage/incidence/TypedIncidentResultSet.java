package org.hypergraphdb.storage.incidence;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.query.impl.FilteredRAResultSet;
import org.hypergraphdb.query.impl.FilteredResultSet;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.Mapping;

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
public final class TypedIncidentResultSet implements HGRandomAccessResult<HGHandle>
{
    private Mapping<byte[], Boolean> typePredicate = new Mapping<byte[], Boolean>()
    {
        public Boolean eval(byte[] h)
        {
            for (int i = 0; i < typeRep.length; i++)
                if (h[i + typeRep.length] != typeRep[i])
                    return false;
            return true;
        }
    };
 
    FilteredRAResultSet<byte[]> rs; 
    byte [] typeRep;
    HGHandle type;
    ByteArrayConverter<HGPersistentHandle> handleConverter;

    private HGHandle toh(byte [] B)
    {
        return handleConverter.fromByteArray(B, 0, typeRep.length);        
    }
    
    public TypedIncidentResultSet(HGRandomAccessResult<byte[]> irs, 
                                  ByteArrayConverter<HGPersistentHandle> handleConverter,
                                  HGHandle type)
    {
        super();
        this.rs = new FilteredRAResultSet<byte[]>(irs, typePredicate, 0);
        this.type = type;
        typeRep = type.getPersistent().toByteArray();
        this.handleConverter = handleConverter;
    }

    public HGHandle current()
    {
        return toh(rs.current());
    }

    public void close()
    {
    }

    public boolean isOrdered()
    {
        return rs.isOrdered();
    }

    public boolean hasPrev()
    {
        return rs.hasPrev();
    }

    public HGHandle prev()
    {
        return toh(rs.prev());
    }

    public boolean hasNext()
    {
        return rs.hasNext();
    }

    public HGHandle next()
    {
        return toh(rs.next());
    }

    public void remove()
    {
        rs.remove();
    }

    public org.hypergraphdb.HGRandomAccessResult.GotoResult goTo(HGHandle value,
                                                                 boolean exactMatch)
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void goAfterLast()
    {
        rs.goAfterLast();
    }

    public void goBeforeFirst()
    {
        rs.goBeforeFirst();
    }        
}