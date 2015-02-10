package hgtest.storage.bje.BJETxLock;

import com.sleepycat.je.DatabaseEntry;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.bje.BJETxLock;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class BJETxLock_getObjectIdTest
{
	@Test
	public void instanceConstructedUsingEmptyByteArray() throws Exception
	{
		final byte[] expected = new byte[] {};

		final HyperGraph graph = PowerMock.createStrictMock(HyperGraph.class);
		PowerMock.replayAll();
		final byte[] objectId = new byte[] {};
		final BJETxLock bjeLock = new BJETxLock(graph, objectId);

		final byte[] actual = bjeLock.getObjectId();

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
		final BJETxLock bjeLock = new BJETxLock(graph, objectId);

		final byte[] actual = bjeLock.getObjectId();

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
		final BJETxLock bjeLock = new BJETxLock(graph, objectId);

		final byte[] actual = bjeLock.getObjectId();

		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}

    @Test
    public void instanceConstructedUsingOneByteDatabaseEntry() throws Exception
    {
        final byte[] expected = new byte[] {2};

        final HyperGraph graph = PowerMock.createStrictMock(HyperGraph.class);
        PowerMock.replayAll();
        final DatabaseEntry objectId = new DatabaseEntry(new byte[] {2});
        final BJETxLock bjeLock = new BJETxLock(graph, objectId);

        final byte[] actual = bjeLock.getObjectId();

        assertEquals(actual, expected);
        PowerMock.verifyAll();
    }

    @Test
    public void instanceConstructedUsingFourBytesDatabaseEntry() throws Exception
    {
        final byte[] expected = new byte[] {2, 4, 9, 16};

        final HyperGraph graph = PowerMock.createStrictMock(HyperGraph.class);
        PowerMock.replayAll();
        final DatabaseEntry objectId = new DatabaseEntry(new byte[] {2, 4, 9, 16});
        final BJETxLock bjeLock = new BJETxLock(graph, objectId);

        final byte[] actual = bjeLock.getObjectId();

        assertEquals(actual, expected);
        PowerMock.verifyAll();
    }
}
