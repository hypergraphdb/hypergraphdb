package org.hypergraphdb.peer.protocol;

import java.io.ByteArrayOutputStream;

public class BoundedByteOutputStream extends ByteArrayOutputStream
{
	private int bound = Integer.MAX_VALUE;
	private int written = 0;
	
	public static final class BoundExceeded extends RuntimeException
	{
		private static final long serialVersionUID = 1656577181978054334L;
		public BoundExceeded(int excess, int bound) 
		{ 
			super("Bounded output exceeded by " + excess + ", limit is " + bound);		
		}
	}
	
	public BoundedByteOutputStream(int bound)
	{
		this.bound = bound;
	}

	@Override
	public synchronized void write(byte[] b, int off, int len)
	{
		if (len + written > bound)
			throw new BoundExceeded(len + written - bound, bound);
		super.write(b, off, len);
		written += len;
	}

	@Override
	public synchronized void write(int b)
	{
		if (1 + written > bound)
			throw new BoundExceeded(1 + written - bound, bound);
		super.write(b);
		written++;
	}
}
