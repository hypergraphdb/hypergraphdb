package org.hypergraphdb.peer.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.storage.HGStoreSubgraph;
import org.hypergraphdb.storage.StorageGraph;

/**
 * @author ciprian.costa Class that serializes objects based on their HGDB
 *         representation
 */
public class GenericSerializer implements HGSerializer
{
    private static HyperGraph tempDB;
    private SubgraphSerializer serializer = new SubgraphSerializer();

    public static HyperGraph getTempDB()
    {
        return tempDB;
    }

    public static void setTempDB(HyperGraph tempDB)
    {
        GenericSerializer.tempDB = tempDB;
    }

    public Object readData(InputStream in) throws IOException
    {
        StorageGraph result = null;
        int subgraphType = 0;
        subgraphType = in.read();

        if (subgraphType == 2)
        {
            return null;
        }
        else if (subgraphType == 0)
        {
            result = (StorageGraph) serializer.readData(in);
            return result;
        }
        else
        {
            result = (StorageGraph) serializer.readData(in);
            SubgraphManager.store(result, tempDB.getStore());
            HGPersistentHandle theRoot = result.getRoots().iterator().next();
            Object resultObj = tempDB.get(theRoot);
            tempDB.remove(theRoot);
            return resultObj;
        }
    }

    public void writeData(OutputStream out, Object data) throws IOException
    {
        StorageGraph subGraph;
        HGPersistentHandle tempHandle = null;

        if (data == null)
        {
            out.write(2);
        }
        else
        {
            if (data instanceof StorageGraph)
            {
                out.write(0);
                subGraph = (StorageGraph) data;
            }
            else
            {
                out.write(1);
                tempHandle = tempDB.getPersistentHandle(tempDB.add(data));
                subGraph = new HGStoreSubgraph(tempHandle, tempDB.getStore());
            }
            serializer.writeData(out, subGraph);
            if (tempHandle != null)
            {
                tempDB.remove(tempHandle);
            }
        }
    }

}
