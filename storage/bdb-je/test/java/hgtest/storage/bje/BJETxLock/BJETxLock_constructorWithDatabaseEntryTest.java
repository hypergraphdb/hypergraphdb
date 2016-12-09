package hgtest.storage.bje.BJETxLock;

import org.hypergraphdb.storage.bje.BJETxLock;
import org.junit.Test;

import com.sleepycat.je.DatabaseEntry;

public class BJETxLock_constructorWithDatabaseEntryTest extends
		BJETxLockTestBasis
{
	@Test
	public void doesNotFail_whenGraphIsNull() throws Exception
	{
		final DatabaseEntry objectId = new DatabaseEntry(new byte[] { 0, 0, 0,
				1 });

		new BJETxLock(null, objectId);
	}

	@Test
	public void throwsException_whenObjectIdIsNull() throws Exception
	{
		final DatabaseEntry objectId = null;

		below.expect(NullPointerException.class);
		new BJETxLock(mockedGraph, objectId);
	}

	@Test
	public void happyPath() throws Exception
	{
		final DatabaseEntry objectId = new DatabaseEntry(new byte[] { 0, 0, 0,
				1 });

		new BJETxLock(mockedGraph, objectId);
	}
}
