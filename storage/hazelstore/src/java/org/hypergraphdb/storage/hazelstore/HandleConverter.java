package org.hypergraphdb.storage.hazelstore;

import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.storage.ByteArrayConverter;

public class HandleConverter implements ByteArrayConverter<HGPersistentHandle> {

    HGHandleFactory hf;

    public HandleConverter(HGHandleFactory hf) {
        this.hf = hf;
    }
    
    
    protected byte[] writeBytes(HGPersistentHandle value)	{
        if (value instanceof HGPersistentHandle)
            return ((HGPersistentHandle)value).toByteArray();
        else
            return ((HGLiveHandle)value).getPersistent().toByteArray();
    }

    
    protected HGPersistentHandle readBytes(byte[] data, int offset) {
        return hf.makeHandle(data, offset);
    }

    
    public byte[] toByteArray(HGPersistentHandle object) {
        if (object instanceof HGPersistentHandle)
            return ((HGPersistentHandle)object).toByteArray();
        else
            return ((HGLiveHandle)object).getPersistent().toByteArray();
    }
    
    
    public HGPersistentHandle fromByteArray(byte[] byteArray, int offset, int length) {
        return hf.makeHandle(byteArray, offset);
    }
}

