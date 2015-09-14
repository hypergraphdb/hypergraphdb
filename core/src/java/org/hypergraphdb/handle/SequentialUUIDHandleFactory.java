package org.hypergraphdb.handle;

import java.util.concurrent.atomic.AtomicLong;

import org.hypergraphdb.HGPersistentHandle;

/**
 * 
 * <p>
 * A handle factory that generates UUID persistent handles out of a base <code>long</code>
 * and an increment counter <code>long</code> value. The base and the seed of the counter
 * can be set at construction time or at any later time. The base value goes into the higher
 * 8 bytes of the UUID and the counter value goes into the lower 8 bytes.  
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class SequentialUUIDHandleFactory extends UUIDHandleFactory
{
    private AtomicLong seed = new AtomicLong(System.currentTimeMillis());
    private byte [] data = new byte[16];
    private long base = 0;
    
    public SequentialUUIDHandleFactory()
    {
        setBase(base);
    }

    public SequentialUUIDHandleFactory(long base, long seed)
    {
        setSeed(seed);
        setBase(base);
    }
    
    public long getBase()
    {
        return base;
    }

    public void setBase(long base)
    {
        this.base = base;
        data[0] = (byte) ((base >>> 56)); 
        data[1] = (byte) ((base >>> 48));
        data[2] = (byte) ((base >>> 40)); 
        data[3] = (byte) ((base >>> 32));
        data[4] = (byte) ((base >>> 24)); 
        data[5] = (byte) ((base >>> 16));
        data[6] = (byte) ((base >>> 8)); 
        data[7] = (byte) ((base >>> 0));        
    }

    public long getSeed()
    {
        return seed.get();
    }
    
    public void setSeed(long newSeed)
    {
        seed.set(newSeed);
    }
    
    public HGPersistentHandle makeHandle()
    {
        long v = seed.incrementAndGet();
        data[8] = (byte) ((v >>> 56)); 
        data[9] = (byte) ((v >>> 48));
        data[10] = (byte) ((v >>> 40)); 
        data[11] = (byte) ((v >>> 32));
        data[12] = (byte) ((v >>> 24)); 
        data[13] = (byte) ((v >>> 16));
        data[14] = (byte) ((v >>> 8)); 
        data[15] = (byte) ((v >>> 0));
        return UUIDPersistentHandle.makeHandle(data);        
    }    
}