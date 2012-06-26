package org.hypergraphdb.pithos;

import java.io.File;

import java.nio.ByteBuffer;
import org.hypergraphdb.HGException;

public class FreeSpace extends FileStore
{
	private int newSpaceOffset = 0;
	private int newBucketOffset = 8;
	private int smallEntriesOffset = 16;
	private int smallEntriesCount = 1024;		
	private int hashTableSize = 1024;
	private int hashTableOffset = smallEntriesOffset + 8*smallEntriesCount;
	private ByteBuffer sizeMap = null;
			
	// We want each bucket to fit a page_size
	private int bucketSize = config.getPageSize();
			
	private class SizeBucket
	{
		long position;
		ByteBuffer buf;
		SizeBucket(long position)  
		{ 
			try
			{
				this.position = position;
				buf = write(position, bucketSize);
			}
			catch (Throwable t) { throw new HGException(t); }
		}
		int count() { return buf.getInt(0); }
		void count(int n) { buf.putInt(0, n); }
		int size() { return buf.getInt(4); }
		void size(int n) { buf.putInt(4, n); }
		int max() { return bucketSize / 8 - 3; }
		void prev(long offset)
		{
			buf.putLong(8, offset);
		}
		SizeBucket prev()
		{
			long prevOffset = buf.getLong(8);
			return prevOffset == 0 ? null : new SizeBucket(prevOffset);
		}
		long prevOffset()
		{
			return buf.getLong(8);
		}
		void next(long offset)
		{
			buf.putLong(16, offset);
		}
		SizeBucket next() 
		{
			long nextOffset = buf.getLong(16);
			return nextOffset == 0 ? null : new SizeBucket(nextOffset);
		}
		long nextOffset()
		{
			return buf.getLong(16);
		}
		long get()
		{
			if (count() == 0)
				throw new IllegalStateException("Bucket empty.");
			buf.putInt(0, count() - 1);
			long result = buf.getLong(24 + 8*count());
			if (count() == 0)
				freeBucket(this);
			return result;
		}
		void put(long offset)
		{
			if (count() == max())
				throw new IllegalStateException("Bucket full.");
			buf.putLong(24 + 8*count(), offset);
			buf.putInt(0, count() + 1);
		}
	}
	
	private void initializeFile()
	{
		try
		{
			int totalmapsize = 1 + smallEntriesCount + hashTableSize;
			sizeMap = write(0, 8*totalmapsize);
			// I have no idea if a new mapped file is 0-initialized, a test
			// show that it is, but the API docs are silent about this.
			// So, to be on the safe side, a quick put to 0...
			for (int i = 0; i < totalmapsize; i++ )
				sizeMap.putLong(0);
			sizeMap.putLong(newBucketOffset, 8*totalmapsize);
		}
		catch (Throwable t)
		{
			throw new HGException(t);
		}
	}
	
	private int sizeIndex(int size)
	{
		return size <= smallEntriesCount ? smallEntriesOffset + size 
										 : hashTableOffset + size % hashTableSize;
				
	}
	private SizeBucket findSizeBucket(int desiredSize)
	{
		long position = sizeMap.getLong(sizeIndex(desiredSize));
		return position == 0 ? null : new SizeBucket(position);
	}
	
	private long reserveNew(int size)
	{
		// if no available block, we allocate at the end of the file
		// as indicated by the offset at sizeMap[0];
		long result = sizeMap.getLong(newSpaceOffset);
		sizeMap.putLong(newSpaceOffset, result + size);
		return result;		
	}
	
	private SizeBucket newBucket(int size)
	{
		long offset = sizeMap.getLong(newBucketOffset);
		SizeBucket bucket = new SizeBucket(offset);
		bucket.count(0);
		bucket.size(size);
		bucket.prev(0);
		bucket.next(0);
		sizeMap.putLong(newBucketOffset, offset + bucketSize);
		return bucket;
	}
	
	private void freeBucket(SizeBucket bucket)
	{
		// First, we need to unlink from the list.
		if (bucket.prevOffset() != 0)
			bucket.prev().next(bucket.nextOffset());
		else
			sizeMap.putLong(sizeIndex(bucket.size()), bucket.nextOffset());
		if (bucket.nextOffset() != 0)
			bucket.next().prev(bucket.prevOffset());
		// Next, if this is not the last bucket block in the file, we want to swap
		// the last into it so as to reclaim that space
		long lastBucketPosition = sizeMap.getLong(newBucketOffset) - bucketSize;
		if (lastBucketPosition != bucket.position)
		{
			byte [] data = new byte[bucketSize];
			try
			{
				read(lastBucketPosition, bucketSize).get(data);
				write(bucket.position, bucketSize).put(data);
			}
			catch (Throwable t)
			{
				throw new HGException(t);
			}
		}
		sizeMap.putLong(newBucketOffset, lastBucketPosition); 
	}
	
	public FreeSpace(File file, PithosConfig config)
	{
		super(file, config);
	}
	
	public void startup()
	{
		boolean isnew = !getFile().exists();
		super.startup();
		if (isnew)
			initializeFile();
		else
		{
			int totalmapsize = 1 + smallEntriesCount + hashTableSize;
			try
			{
				sizeMap = write(0, 8*totalmapsize);
			}
			catch (Throwable t)
			{
				throw new HGException(t);
			}
		}
	}
	
	public void shutdown()
	{
		super.shutdown();
	}
	
	public long findAvailable(int size)
	{
		for (SizeBucket bucket = findSizeBucket(size); bucket != null; bucket = bucket.next())
			if (bucket.size() == size && bucket.count() > 0)
				return bucket.get();
		return -1;
	}
	
	public long reserve(int size)
	{
		long result = findAvailable(size);
		if (result > -1)
			return result;
		else
			return reserveNew(size);
	}
	
	public void free(long position, int size)
	{
		SizeBucket bucket = findSizeBucket(size);
		if (bucket == null)
		{
			// create a new bucket with that size and put it as the first element of 
			// bucket list for that size
			bucket = newBucket(size);
			sizeMap.putLong(sizeIndex(size), bucket.position);
		}
		else if (bucket.count() == bucket.max())
		{
			SizeBucket newb = newBucket(size);
			newb.prev(bucket.position);
			newb.next(bucket.nextOffset());
			bucket.next(newb.position);
			bucket = newb;
		}
		bucket.put(position);
	}

	void test1() throws Exception
	{
		write(0, 100).putDouble(3.14);
	}
	
	void test2() throws Exception
	{
		System.out.println(read(0, 100).getDouble());
	}
	
	public static void main(String [] argv)
	{
		FreeSpace fs = new FreeSpace(new File("/home/borislav/tmp/freespacetest"), new PithosConfig());
		fs.startup();
		try
		{
			long p = fs.reserve(50);
			long p2 = fs.reserve(1000);
			long p3 = fs.reserve(1000);
			System.out.println("" + p +"," + p2 + "," + p3);
			fs.free(p, 50);
		}
		catch (Throwable t)
		{
			t.printStackTrace();
		}
		finally
		{
			fs.shutdown();
		}
	}
}