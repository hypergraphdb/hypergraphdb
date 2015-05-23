package hgtest.storage.bdb.BDBTxLock;

import com.sleepycat.db.DatabaseEntry;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.bdb.BDBTxLock;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class BDBTxLock_getObjectIdTest
{
	@Test
	public void instanceConstructedUsingEmptyByteArray() throws Exception
	{
		final byte[] expected = new byte[] {};

		final HyperGraph graph = PowerMock.createStrictMock(HyperGraph.class);
		PowerMock.replayAll();
		final byte[] objectId = new byte[] {};
		final BDBTxLock bdbLock = new BDBTxLock(graph, objectId);

		final byte[] actual = bdbLock.getObjectId();

		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}

	@Test
	public void instanceConstructedUsingOneItemByteArray() throws Exception
	{
		final byte[] expected = new byte[] { 1 };

		final HyperGraph graph = PowerMock.createStrictMock(HyperGraph.class);
		PowerMock.replayAll();
		final byte[] objectId = new byte[] { 1 };
		final BDBTxLock bdbLock = new BDBTxLock(graph, objectId);

		final byte[] actual = bdbLock.getObjectId();

		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}

	@Test
	public void instanceConstructedUsingFourItemsByteArray() throws Exception
	{
		final byte[] expected = new byte[] { 2, 3, 4, 5 };

		final HyperGraph graph = PowerMock.createStrictMock(HyperGraph.class);
		PowerMock.replayAll();
		final byte[] objectId = new byte[] { 2, 3, 4, 5 };
		final BDBTxLock bdbLock = new BDBTxLock(graph, objectId);

		final byte[] actual = bdbLock.getObjectId();

		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}

	@Test
	public void instanceConstructedUsingOneByteDatabaseEntry() throws Exception
	{
		final byte[] expected = new byte[] { 2 };

		final HyperGraph graph = PowerMock.createStrictMock(HyperGraph.class);
		PowerMock.replayAll();
		final DatabaseEntry objectId = new DatabaseEntry(new byte[] { 2 });
		final BDBTxLock bdbLock = new BDBTxLock(graph, objectId);

		final byte[] actual = bdbLock.getObjectId();

		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}

	@Test
	public void instanceConstructedUsingFourBytesDatabaseEntry()
			throws Exception
	{
		final byte[] expected = new byte[] { 2, 4, 9, 16 };

		final HyperGraph graph = PowerMock.createStrictMock(HyperGraph.class);
		PowerMock.replayAll();
		final DatabaseEntry objectId = new DatabaseEntry(new byte[] { 2, 4, 9,
				16 });
		final BDBTxLock bdbLock = new BDBTxLock(graph, objectId);

		final byte[] actual = bdbLock.getObjectId();

		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}
}
