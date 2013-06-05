package org.hypergraphdb.storage.incidence;

import org.hypergraphdb.HGException;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.storage.BAtoHandle;

public class TypeAndPositionIncidenceAnnotator implements HGIncidentAnnotator
{

    public int spaceNeeded(HyperGraph graph)
    {        
        return 4 + graph.getHandleFactory().anyHandle().toByteArray().length;
    }

    public void annotate(HyperGraph graph, 
                         HGHandle linkHandle, 
                         HGHandle targetHandle,
                         byte[] data, 
                         int offset)
    {
        HGLink link = graph.get(linkHandle);
        int targetPosition = -1;
        for (int i = 0; i < link.getArity() && targetPosition < 0; i++)
            if (targetHandle.equals(link.getTargetAt(i)))
                targetPosition = i;
        if (targetPosition < 0)
            throw new IllegalArgumentException("Target " + targetHandle + " not found in link " + linkHandle);
        HGHandle linkType = graph.getType(linkHandle);
        byte [] type = linkType.getPersistent().toByteArray();
        System.arraycopy(type, 0, data, offset, type.length);
        BAUtils.writeInt(targetPosition, data, offset + type.length);
    }
    
    
    public HGRandomAccessResult<HGHandle> lookup(HyperGraph graph, HGRandomAccessResult<byte[]> rs, Object...annotations)
    {
        return new TypedIncidentResultSet(rs, 
                    BAtoHandle.getInstance(graph.getHandleFactory()), 
                    (HGHandle)annotations[0]);
    }
    
    /**
     * <p>
     * Expected annotation arguments: 1st is the type of the link and 2nd the 
     * position of the target within the link. If the type is null
     * </p>
     */
    public byte [] annotateLookup(HyperGraph graph, HGHandle target, Object...annotations)
    {
        HGHandle type = null;
        Integer position = null;            
        if (annotations.length > 0)
            type = (HGHandle)annotations[0];
        byte [] targetKey = target.getPersistent().toByteArray();
        if (annotations.length > 1)
            position = (Integer)annotations[1];
        if (type == null && position != null)
            throw new HGException("Type of link must be specified alongside target position for a typed incident lookup.");
        else if (type != null && position != null)
        {
            byte [] key = new byte[2*targetKey.length + 4];
            System.arraycopy(targetKey, 0, key, 0, targetKey.length);
            System.arraycopy(type.getPersistent().toByteArray(), 0, key, targetKey.length, targetKey.length);
            BAUtils.writeInt(position, key, 2*targetKey.length);
            return key;
        }
        else if (type != null)
        {
            byte [] key = new byte[2*targetKey.length];
            System.arraycopy(targetKey, 0, key, 0, targetKey.length);
            System.arraycopy(type.getPersistent().toByteArray(), 0, key, targetKey.length, targetKey.length);
            return key;
        }
        else
            return targetKey;
    }
}