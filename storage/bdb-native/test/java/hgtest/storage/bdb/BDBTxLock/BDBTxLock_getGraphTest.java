package hgtest.storage.bdb.BDBTxLock;

import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.bdb.BDBTxLock;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class BDBTxLock_getGraphTest
{
	@Test
	public void graphIsNull() throws Exception
	{
		final byte[] objectId = new byte[] { 0, 0, 0, 1 };
		final BDBTxLock bdbLock = new BDBTxLock(null, objectId);

		final HyperGraph actual = bdbLock.getGraph();

		assertNull(actual);
	}

	@Test
	public void graphIsNotNull() throws Exception
	{
		final HyperGraph expectedGraph = PowerMock
				.createStrictMock(HyperGraph.class);
		PowerMock.replayAll();
		final byte[] objectId = new byte[] { 0, 0, 0, 1 };
		final BDBTxLock bdbLock = new BDBTxLock(expectedGraph, objectId);

		final HyperGraph actualGraph = bdbLock.getGraph();

		assertEquals(actualGraph, expectedGraph);
		PowerMock.verifyAll();
	}
}
