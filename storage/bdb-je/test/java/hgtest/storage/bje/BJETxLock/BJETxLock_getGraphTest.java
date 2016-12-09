package hgtest.storage.bje.BJETxLock;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.bje.BJETxLock;
import org.junit.Test;

public class BJETxLock_getGraphTest extends BJETxLockTestBasis
{
	@Test
	public void returnsNullInstance_whenGraphIsNull() throws Exception
	{
		final byte[] objectId = new byte[] { 0, 0, 0, 1 };
		final BJETxLock bjeLock = new BJETxLock(null, objectId);

		final HyperGraph actual = bjeLock.getGraph();

		assertNull(actual);
	}

	@Test
	public void returnsTheSameInstance_whenGraphIsNotNull() throws Exception
	{
		final HyperGraph expectedGraph = mockedGraph;

		final byte[] objectId = new byte[] { 0, 0, 0, 1 };
		final BJETxLock bjeLock = new BJETxLock(expectedGraph, objectId);

		final HyperGraph actualGraph = bjeLock.getGraph();

		assertSame(expectedGraph, actualGraph);
	}
}
