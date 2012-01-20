/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.storage.HGStoreSubgraph;
import org.hypergraphdb.storage.StorageGraph;

/**
 * @author ciprian.costa Class that serializes objects based on their HGDB
 *         representation
 */
public class GenericSerializer implements HGSerializer
{
	private static final int NULL_MARKER = 2;
	private static final int TRANSIENT_SUBGRAPH = 0;
	private static final int SAVED_SUBGRAPH = 1;
	private static final int BYTE_BUFFER = 3;
	
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
        int type = in.read();

        switch (type)
        {
        	case NULL_MARKER: 
        	{
        		return null;
        	}
        	case TRANSIENT_SUBGRAPH:
        	{
        		return (StorageGraph) serializer.readData(in);
        	}
        	case SAVED_SUBGRAPH:
        	{
                result = (StorageGraph) serializer.readData(in);
                SubgraphManager.store(result, tempDB.getStore());
                HGPersistentHandle theRoot = result.getRoots().iterator().next();
                Object resultObj = tempDB.get(theRoot);
                tempDB.remove(theRoot);
                return resultObj;        		
        	}
        	case BYTE_BUFFER:
        	{
        		byte [] sizeB = new byte[4];
        		in.read(sizeB, 0, 4);
        		int size = BAUtils.readInt(sizeB, 0);
        		byte [] buf = new byte[size];        		
        		for (int sofar = 0; sofar < size; )
        		{
        			int read = in.read(buf, sofar, size - sofar);
        			if (read == -1)
        				throw new RuntimeException("Unexpected end of message stream ,expecting a byte[] of size " + 
        						size + ", but only got " + sofar + " bytes.");
        			sofar += read;
        		}
        		return buf;
        	}
        	default:
        		throw new RuntimeException("Unknown serialization marker " + type);
        }
    }

    public void writeData(OutputStream out, Object data) throws IOException
    {
        StorageGraph subGraph;
        HGPersistentHandle tempHandle = null;

        if (data == null)
        {
            out.write(NULL_MARKER);
            return;
        }
        else if (data instanceof byte[])
        {
        	out.write(BYTE_BUFFER);
        	byte [] len = new byte[4];
        	BAUtils.writeInt(((byte[]) data).length, len, 0);
        	out.write(len);
        	out.write((byte[])data);
        	return;
        }
        
        if (data instanceof StorageGraph)
        {
            out.write(TRANSIENT_SUBGRAPH);
            subGraph = (StorageGraph) data;
        }
        else // some other value that we try to serialize in the temp HGDB
        {
            out.write(SAVED_SUBGRAPH);
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