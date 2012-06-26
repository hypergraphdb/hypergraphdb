package org.hypergraphdb.pithos;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.hypergraphdb.HGException;

public class FileStore
{
	protected PithosConfig config;
	private File file = null;
	private RandomAccessFile raf = null;
	private FileChannel fch = null;
	
	protected FileStore(File file, PithosConfig config)
	{
		this.file = file;
		this.config = config;
	}
	
	protected ByteBuffer read(long offset, int size) throws IOException
	{
		return fch.map(MapMode.READ_ONLY, offset, size);	
	}
	
	protected ByteBuffer write(long offset, int size) throws IOException
	{
		return fch.map(MapMode.READ_WRITE, offset, size);
	}
	
	public File getFile()
	{
		return file;
	}
	
	public void startup()
	{
		try
		{
			raf = new RandomAccessFile(file, "rw");
			fch = raf.getChannel();
		}
		catch (Throwable t)
		{
			throw new HGException(t);
		}
	}

	public void shutdown()
	{
		try 
		{ 
			fch.close();
			raf.close(); 
		}
		catch (Throwable t)
		{
			t.printStackTrace();
		}
	}
	
}