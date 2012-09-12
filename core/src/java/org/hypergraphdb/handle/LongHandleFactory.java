package org.hypergraphdb.handle;

import java.util.concurrent.atomic.AtomicLong;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.storage.BAUtils;

public class LongHandleFactory implements HGHandleFactory
{
    private static final LongPersistentHandle any = new LongPersistentHandle(1);
    private static final LongPersistentHandle nil = new LongPersistentHandle(0);
    private static final LongPersistentHandle topType = new LongPersistentHandle(100);
    private static final LongPersistentHandle linkType = new LongPersistentHandle(101);
    private static final LongPersistentHandle nullType = new LongPersistentHandle(102);
    private static final LongPersistentHandle subsumesType = new LongPersistentHandle(103);
    
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
    
    public HGPersistentHandle topTypeHandle()
    {
        return topType;
    }

    public HGPersistentHandle nullTypeHandle()
    {
        return nullType;
    }

    public HGPersistentHandle linkTypeHandle()
    {
        return linkType;
    }

    public HGPersistentHandle subsumesTypeHandle()
    {
        return subsumesType;
    }    
}