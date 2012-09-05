package org.hypergraphdb.indexing;

import java.util.Comparator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.indexing.HGKeyIndexer;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.type.HGPrimitiveType;

/**
 * <p>
 * Index atoms directly by their values serialized as byte[]. The type of the atoms
 * indexed is assumed to be an instance of {@link HGPrimitiveType} so that a 
 * {@link ByteArrayConverter} and a <code>Comparator</code> are made available.
 * </p>
 * 
 * @author borislav
 *
 * @param <AtomType>
 */
public class DirectValueIndexer<AtomType> extends HGKeyIndexer<AtomType>
{
    public DirectValueIndexer()
    {

    }

    public DirectValueIndexer(HGHandle type)
    {
        super(type);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ByteArrayConverter<AtomType> getConverter(HyperGraph graph)
    {
        return (ByteArrayConverter<AtomType>) graph.get(this.getType());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Comparator<byte[]> getComparator(HyperGraph graph)
    {
        return ((HGPrimitiveType) graph.get(this.getType())).getComparator();
    }

    @SuppressWarnings("unchecked")
    @Override
    public AtomType getKey(HyperGraph graph, Object atom)
    {
        return (AtomType) atom;
    }

    public int hashCode()
    {
        return getType().hashCode();
    }

    @SuppressWarnings("unchecked")
    public boolean equals(Object x)
    {
        if (!(x instanceof DirectValueIndexer)) return false;
        return getType().equals(((DirectValueIndexer<AtomType>) x).getType());
    }
}