package hgtest.storage.bje.BJETxLock;

import static org.junit.Assert.assertArrayEquals;

import org.hypergraphdb.storage.bje.BJETxLock;
import org.junit.Test;

import com.sleepycat.je.DatabaseEntry;

public class BJETxLock_getObjectIdTest extends BJETxLockTestBasis
{
	@Test
	public void returnsEmptyArray_whenLockHasBeenConstructedUsingEmptyArray()
			throws Exception
	{
		final byte[] expected = new byte[] {};

		final BJETxLock bjeLock = new BJETxLock(mockedGraph, new byte[] {});

		final byte[] actual = bjeLock.getObjectId();
		assertArrayEquals(expected, actual);
	}

	@Test
	public void returnsCorrespondingArray_whenLockHasBeenConstructedUsingOneItemArray()
			throws Exception
	{
		final byte[] expected = new byte[] { 1 };

		final BJETxLock bjeLock = new BJETxLock(mockedGraph, new byte[] { 1 });

		final byte[] actual = bjeLock.getObjectId();
		assertArrayEquals(expected, actual);
	}

	@Test
	public void returnsCorrespondingArray_whenLockHasBeenConstructedUsingLongArray()
			throws Exception
	{
		final byte[] expected = new byte[] { 22, 33, 44, 55, 66, 77, 88, 99, 10 };

		final BJETxLock bjeLock = new BJETxLock(mockedGraph, new byte[] { 22,
				33, 44, 55, 66, 77, 88, 99, 10 });

		final byte[] actual = bjeLock.getObjectId();
		assertArrayEquals(expected, actual);
	}

	@Test
	public void returnsCorrespondingArray_whenLockHasBeenConstructedUsingOneByteDatabaseEntry()
			throws Exception
	{
		final byte[] expected = new byte[] { 2 };

		final DatabaseEntry objectId = new DatabaseEntry(new byte[] { 2 });
		final BJETxLock bjeLock = new BJETxLock(mockedGraph, objectId);

		final byte[] actual = bjeLock.getObjectId();
		assertArrayEquals(expected, actual);
	}

	@Test
	public void instanceConstructedUsingManyBytesDatabaseEntry()
			throws Exception
	{
		final byte[] expected = new byte[] { 22, 33, 44, 55, 66, 77, 88, 99, 10 };

		final DatabaseEntry objectId = new DatabaseEntry(new byte[] { 22, 33,
				44, 55, 66, 77, 88, 99, 10 });
		final BJETxLock bjeLock = new BJETxLock(mockedGraph, objectId);

		final byte[] actual = bjeLock.getObjectId();
		assertArrayEquals(expected, actual);
	}
}
