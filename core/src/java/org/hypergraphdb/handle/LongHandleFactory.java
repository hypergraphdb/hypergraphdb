package org.hypergraphdb.handle;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.storage.BAUtils;

/**
 * 
 * <p>
 * A handle factory where identifiers are Java long numbers, sequentially incremented. This is suitable
 * either for a local installation, or for a distributed installation where data is written to a single
 * node so no conflicting identifiers are possible.
 * </p>
 *
 * <p>
 * <b>Important note</b>: because the predefined type system already declares a few atoms as part of the initialization
 * process, when configuring a newly created database with this handle factory, one must also tell the type system
 * to use a different configuration file for the predefined types. Here is how to do it:
 * 
 * </p>
 * 
 * <code><pre>
 * HGConfiguration config = new HGConfiguration();
 *	((JavaTypeSchema)config.getTypeConfiguration().getDefaultSchema()).setPredefinedTypes("/org/hypergraphdb/types_intid");
 * LongHandleFactory hgHandleFactory = new LongHandleFactory();
 * config.setHandleFactory(hgHandleFactory);	
 * HyperGraph graph = HGEnvironment.get(location, config);
 * </pre></code>
 * 
 * @author Borislav Iordanov
 *
 */
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

    @Override
	public HGPersistentHandle makeHandle(InputStream in)
	{
		try
		{
			return new LongPersistentHandle(new DataInputStream(in).readLong());
		}
		catch (IOException e)
		{
			throw new HGException(e);
		}
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