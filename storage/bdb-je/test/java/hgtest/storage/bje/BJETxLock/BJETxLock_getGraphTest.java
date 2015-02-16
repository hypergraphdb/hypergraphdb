package hgtest.storage.bje.BJETxLock;

import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.bje.BJETxLock;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class BJETxLock_getGraphTest
{
	@Test
	public void graphIsNull() throws Exception
	{
		final byte[] objectId = new byte[] { 0, 0, 0, 1 };
		final BJETxLock bjeLock = new BJETxLock(null, objectId);

		final HyperGraph actual = bjeLock.getGraph();

		assertNull(actual);
	}

	@Test
	public void graphIsNotNull() throws Exception
	{
		final HyperGraph expectedGraph = PowerMock
				.createStrictMock(HyperGraph.class);
		PowerMock.replayAll();
		final byte[] objectId = new byte[] { 0, 0, 0, 1 };
		final BJETxLock bjeLock = new BJETxLock(expectedGraph, objectId);

		final HyperGraph actualGraph = bjeLock.getGraph();

		assertEquals(actualGraph, expectedGraph);
		PowerMock.verifyAll();
	}
}
