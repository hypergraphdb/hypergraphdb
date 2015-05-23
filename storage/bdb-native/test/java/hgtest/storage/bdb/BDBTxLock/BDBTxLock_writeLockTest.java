package hgtest.storage.bdb.BDBTxLock;

import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.bdb.BDBTxLock;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.util.concurrent.locks.Lock;

import static org.testng.Assert.assertNotNull;

/**
 * @author Yuriy Sechko
 */
public class BDBTxLock_writeLockTest
{
	@Test
	public void test() throws Exception
	{
		final HyperGraph graph = PowerMock.createStrictMock(HyperGraph.class);
		PowerMock.replayAll();
		final byte[] objectId = new byte[] { 0, 0, 0, 1 };
		final BDBTxLock bdbLock = new BDBTxLock(graph, objectId);

		final Lock writeLock = bdbLock.writeLock();

		assertNotNull(writeLock);
	}
}
