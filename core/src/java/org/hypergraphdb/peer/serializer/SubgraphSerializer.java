package org.hypergraphdb.peer.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.storage.RAMStorageGraph;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.util.Pair;

public class SubgraphSerializer implements SerializerMapper, HGSerializer
{
    private static byte OBJECT_DATA = 0;
    private static byte LINK_DATA = 1;
    private static byte END = 2;

    public HGSerializer accept(Class<?> clazz)
    {
        if (StorageGraph.class.isAssignableFrom(clazz))
            return this;
        else
            return null;
    }

    public HGSerializer getSerializer()
    {
        return this;
    }

    public Object readData(InputStream in) throws IOException
    {
        RAMStorageGraph result = null;

        byte type = 0;
        
        int rootCount = in.read();
        Set<HGPersistentHandle> roots = new HashSet<HGPersistentHandle>();
        while (rootCount-- > 0)
            roots.add(PersistentHandlerSerializer.deserializePersistentHandle(in));
        
        result = new RAMStorageGraph(roots);
        
        do
        {
            type = (byte) in.read();
            if (type != END)
            {
                HGPersistentHandle handle = PersistentHandlerSerializer.deserializePersistentHandle(in);
                int length = SerializationUtils.deserializeInt(in);

                if (type == OBJECT_DATA)
                {
                    byte[] value = new byte[length];
                    in.read(value);
                    result.put(handle, value);
                }
                else
                {
                    HGPersistentHandle[] link = new HGPersistentHandle[length];
                    for (int i = 0; i < length; i++)
                    {
                        link[i] = PersistentHandlerSerializer.deserializePersistentHandle(in);
                    }
                    result.put(handle, link);

                }
            }
        } while (type != END);
        return result;
    }

    public void writeData(OutputStream out, Object data) throws IOException
    {
        StorageGraph subgraph = (StorageGraph) data;
        Iterator<Pair<HGPersistentHandle, Object>> iter = subgraph.iterator();

        out.write(subgraph.getRoots().size());
        
        for (HGPersistentHandle root : subgraph.getRoots())
            PersistentHandlerSerializer.serializePersistentHandle(out, root);
        
        while (iter.hasNext())
        {
            Pair<HGPersistentHandle, Object> item = iter.next();
            Object value = item.getSecond();
            
            byte[] byteValue = null;
            HGPersistentHandle[] link = null;

            // write type
            if (value instanceof byte[])
            {
                byteValue = (byte[]) value;
                out.write(OBJECT_DATA);
            }
            else
            {
                link = (HGPersistentHandle[]) value;
                out.write(LINK_DATA);
            }

            // write data
            PersistentHandlerSerializer.serializePersistentHandle(out,
                                                                  item.getFirst());
            if (byteValue != null)
            {
                SerializationUtils.serializeInt(out, byteValue.length);
                out.write(byteValue);
            }
            else
            {
                SerializationUtils.serializeInt(out, link.length);

                // write data
                for (HGPersistentHandle handle : link)
                {
                    PersistentHandlerSerializer.serializePersistentHandle(out,
                                                                          handle);
                }
            }
        }

        out.write(END);
    }
}
