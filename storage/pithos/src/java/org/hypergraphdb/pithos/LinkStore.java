package org.hypergraphdb.pithos;

import java.io.File;
import java.nio.ByteBuffer;

import org.hypergraphdb.HGException;

public class LinkStore extends FileStore
{
	private FreeSpace freeSpace = null;
	
	public LinkStore(File file, PithosConfig config)
	{
		super(file, config);
	}
	
	public void startup()
	{
		super.startup();
		freeSpace = new FreeSpace(new File(getFile().getAbsolutePath() + ".free"), config);
	}
		
	public void writeLink(UPHandle handle, UPHandle [] link)
	{		
		try
		{
			ByteBuffer buf = null;
			long position = -1;
			if (link.length > 2)
			{
				int minBlockSize = (link.length  - 2) * config.getHandleSize();
				position = freeSpace.reserve(minBlockSize);				
				buf = write(position, minBlockSize);
				for (int i = 2; i < link.length; i++)
				{
					buf.putLong(link[i].getGlobal());
					buf.putLong(link[i].getLocal());					
				}
			}
			buf = write(handle.getLocal(),					   	  
					   	  4 + config.getHandleSize()*2 + config.getPointerSize());
			buf.putInt(link.length);
			if (link.length > 0)
			{
				buf.putLong(link[0].getGlobal()).putLong(link[0].getLocal());
				if (link.length > 1)
					buf.putLong(link[1].getGlobal()).putLong(link[1].getLocal());
			}
			if (link.length > 0)
			{
				buf.putLong(position);
			}
		}
		catch (Exception ex)
		{
			throw new HGException(ex);
		}
	}
	
	public void removeLink(UPHandle handle)
	{
		// TODO...
	}
	
	public UPHandle [] readLink(UPHandle handle)
	{
		try
		{
			ByteBuffer buf = null;
			buf = read(4 + handle.getLocal(), config.getHandleSize()*2 + config.getPointerSize());
			int size = buf.getInt();
			UPHandle [] result = new UPHandle[size];
			if (size > 0)
			{
				result[0] = new UPHandle(buf.getLong(), buf.getLong());
				if (size > 1)
					result[1] = new UPHandle(buf.getLong(), buf.getLong()); 
			}
			if (size > 2)
			{
				long position = buf.getLong();
				buf = read(position, (size - 2) * config.getHandleSize());
				for (int i = 2; i < size; i++)
					result[i] = new UPHandle(buf.getLong(), buf.getLong());
			}
			return result;
		}
		catch (Exception ex)
		{
			throw new HGException(ex);
		}
	}
	
	public void shutdown()
	{
		freeSpace.shutdown();		
		super.shutdown();
	}
	
	public static void main(String [] argv)
	{
		PithosHandleFactory hfactory = new PithosHandleFactory();
		LinkStore store = new LinkStore(new File("/tmp/linkstore"), new PithosConfig());
		store.startup();
		try
		{
			UPHandle [] A = new UPHandle[2];
			A[0] = hfactory.makeHandle();
			A[1] = hfactory.makeHandle();
			store.writeLink(hfactory.makeHandle(), A);
		}
		catch (Throwable t)
		{
			t.printStackTrace();
			store.shutdown();
		}
	}
}