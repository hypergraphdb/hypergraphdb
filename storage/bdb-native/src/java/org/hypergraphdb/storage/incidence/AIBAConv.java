package org.hypergraphdb.storage.incidence;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.storage.ByteArrayConverter;

public class AIBAConv implements ByteArrayConverter<HGPersistentHandle>
{
    private ByteArrayConverter<HGPersistentHandle> handleConverter;
    private int handleLength;
    
    public AIBAConv(ByteArrayConverter<HGPersistentHandle> handleConverter, int handleLength)
    {
        this.handleConverter = handleConverter;
        this.handleLength = handleLength;
    }
    
    public byte[] toByteArray(HGPersistentHandle object)
    {
        throw new UnsupportedOperationException();
    }

    public HGPersistentHandle fromByteArray(byte[] byteArray, int offset, int length)
    {
        return handleConverter.fromByteArray(byteArray, offset, handleLength);
    }    
}