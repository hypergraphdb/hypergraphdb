package org.hypergraphdb.handle;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.storage.BAUtils;

public class LongPersistentHandle implements HGPersistentHandle
{
    private static final long serialVersionUID = -2455447560268299073L;
    
    byte [] data = new byte[8];
    
    private long value()
    {
      return BAUtils.readLong(data, 0);        
    }
    
    public LongPersistentHandle(long v)
    {
      BAUtils.writeLong(v, data, 0);        
    }
    
    public byte[] toByteArray()
    {
        return data;
    }

    public int compareTo(HGPersistentHandle other)
    {
        long c = value() - ((LongPersistentHandle)other).value();
        return c < 0 ? -1 : (c > 0 ? 1 : 0);
    }
    
    public int hashCode()
    {
        long v = value();
        return (int) (v ^ (v >>> 32));
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (obj instanceof HGLiveHandle)
            obj = ((HGLiveHandle)obj).getPersistent();        
        if (getClass() != obj.getClass())
            return false;
        LongPersistentHandle other = (LongPersistentHandle) obj;
        return value() == other.value();
    }

    public HGPersistentHandle getPersistent()
    {
        return this;
    }
    
    public String toString()
    {
        return "longHandle(" + Long.toString(value()) + ")";
    }

    public String toStringValue()
    {
        return Long.toString(value());
    }
}