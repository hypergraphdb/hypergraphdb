package hgtest.storage.bdb.BDBTxLock;

import com.sleepycat.db.DatabaseEntry;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.bdb.BDBTxLock;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BDBTxLock_constructor2Test
{
	@Test
	public void graphIsNull() throws Exception
	{
		final DatabaseEntry objectId = new DatabaseEntry(new byte[] { 0, 0, 0,
				1 });

		// no exception here
		new BDBTxLock(null, objectId);
	}

	@Test
	public void objectIdIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		final HyperGraph graph = PowerMock.createStrictMock(HyperGraph.class);
		PowerMock.replayAll();
		final DatabaseEntry objectId = null;

		try
		{
			new BDBTxLock(graph, objectId);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			PowerMock.verifyAll();
		}
	}

	@Test
	public void allIsOk() throws Exception
	{
		final HyperGraph graph = PowerMock.createStrictMock(HyperGraph.class);
		PowerMock.replayAll();
		final DatabaseEntry objectId = new DatabaseEntry(new byte[] { 0, 0, 0,
				1 });

		new BDBTxLock(graph, objectId);

		PowerMock.verifyAll();
	}
}
