package org.hypergraphdb.handle;

import java.util.concurrent.atomic.AtomicLong;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.storage.BAUtils;

public class LongHandleFactory implements HGHandleFactory
{
    private static final LongPersistentHandle any = new LongPersistentHandle(1);
    private static final LongPersistentHandle nil = new LongPersistentHandle(0);
    
    private AtomicLong next = new AtomicLong(1000);
     
    public long getNext()
    {
        return next.get();
    }

    public void setNext(long next)
    {
        this.next.set(next);
    }

    public HGPersistentHandle anyHandle()
    {
        return any;
    }

    public HGPersistentHandle makeHandle()
    {
        return new LongPersistentHandle(next.getAndIncrement());
    }

    public HGPersistentHandle makeHandle(String handleAsString)
    {
        return new LongPersistentHandle(Long.parseLong(handleAsString));
    }

    public HGPersistentHandle makeHandle(byte[] buffer)
    {
        return new LongPersistentHandle(BAUtils.readLong(buffer, 0));
    }

    public HGPersistentHandle makeHandle(byte[] buffer, int offset)
    {
        return new LongPersistentHandle(BAUtils.readLong(buffer, offset));
    }

    public HGPersistentHandle nullHandle()
    {
        return nil;
    }
}