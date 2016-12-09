package hgtest.storage.bje.BJETxLock;

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.locks.Lock;

import org.hypergraphdb.storage.bje.BJETxLock;
import org.junit.Test;

public class BJETxLock_writeLockTest extends BJETxLockTestBasis
{
	@Test
	public void checkThatNotNullLockIsReturned() throws Exception
	{
		final BJETxLock bjeLock = new BJETxLock(mockedGraph, new byte[] { 0 });

		final Lock writeLock = bjeLock.writeLock();

		assertNotNull(writeLock);
	}
}
