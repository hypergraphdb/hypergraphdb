package org.hypergraphdb.handle;

import org.hypergraphdb.HGPersistentHandle;

public class LongPersistentHandle implements HGPersistentHandle
{
    private static final long serialVersionUID = -2455447560268299073L;
    
    byte [] data = new byte[8];
    
    private long value()
    {
        return ((long)data[0] << 56) +
                ((long)(data[1] & 255) << 48) +
                ((long)(data[2] & 255) << 40) +
                ((long)(data[3] & 255) << 32) +
                ((long)(data[4] & 255) << 24) +
                ((data[5] & 255) << 16) + 
                ((data[6] & 255) <<  8) + 
                ((data[7] & 255) <<  0);
    }
    
    public LongPersistentHandle(long v)
    {
        data[0] = (byte) ((v >>> 56)); 
        data[1] = (byte) ((v >>> 48));
        data[2] = (byte) ((v >>> 40)); 
        data[3] = (byte) ((v >>> 32));
        data[4] = (byte) ((v >>> 24)); 
        data[5] = (byte) ((v >>> 16));
        data[6] = (byte) ((v >>> 8)); 
        data[7] = (byte) ((v >>> 0));        
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
            obj = ((HGLiveHandle)obj).getPersistentHandle();        
        if (getClass() != obj.getClass())
            return false;
        LongPersistentHandle other = (LongPersistentHandle) obj;
        return value() == other.value();
    }    
}