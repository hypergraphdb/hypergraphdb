package hgtest.storage.bdb.SingleValueResultSet;

import com.sleepycat.db.DatabaseEntry;
import hgtest.storage.bdb.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bdb.BDBTxCursor;
import org.hypergraphdb.storage.bdb.SingleValueResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class SingleValueResultSet_goToExceptionsTest extends
		SingleValueResultSet_goToTestBasis
{
	@Test
	public void valueIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		TestUtils.putKeyValuePair(environment, database, 1, 11);
		startupCursor();
		final BDBTxCursor fakeCursor = PowerMock
				.createStrictMock(BDBTxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 0, 0, 0, 0 });
		final SingleValueResultSet<Integer> resultSet = new SingleValueResultSet<Integer>(
				fakeCursor, key, converter);

		try
		{
			resultSet.goTo(null, true);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			shutdownCursor();
		}
	}

	@Test
	public void bjeCursorThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"java.lang.IllegalStateException: This exception is throws by fake BJE cursor.");

		TestUtils.putKeyValuePair(environment, database, 1, 11);
		startupCursor();
		final BDBTxCursor fakeCursor = PowerMock
				.createStrictMock(BDBTxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
		EasyMock.expect(fakeCursor.cursor()).andThrow(
				new IllegalStateException(
						"This exception is throws by fake BJE cursor."));
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 0, 0, 0, 0 });
		final SingleValueResultSet<Integer> resultSet = new SingleValueResultSet<Integer>(
				fakeCursor, key, converter);

		try
		{
			resultSet.goTo(1, true);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			shutdownCursor();
		}
	}
}
