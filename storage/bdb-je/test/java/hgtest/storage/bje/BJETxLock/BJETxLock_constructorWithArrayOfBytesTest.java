package hgtest.storage.bje.BJETxLock;

import org.hypergraphdb.storage.bje.BJETxLock;
import org.junit.Test;

public class BJETxLock_constructorWithArrayOfBytesTest extends
		BJETxLockTestBasis
{
	@Test
	public void doesNotFail_whenGraphIsNull() throws Exception
	{
		final byte[] objectId = new byte[] { 0, 0, 0, 1 };

		new BJETxLock(null, objectId);
	}

	@Test
	public void throwsException_whenObjectIdIsNull() throws Exception
	{
		final byte[] objectId = null;

		below.expect(NullPointerException.class);
		new BJETxLock(mockedGraph, objectId);
	}

	@Test
	public void happyPath() throws Exception
	{
		final byte[] objectId = new byte[] { 0, 0, 0, 1 };

		new BJETxLock(mockedGraph, objectId);
	}
}
