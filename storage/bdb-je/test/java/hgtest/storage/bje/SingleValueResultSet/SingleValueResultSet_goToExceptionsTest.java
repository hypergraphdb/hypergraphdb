package hgtest.storage.bje.SingleValueResultSet;

import com.sleepycat.je.DatabaseEntry;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.SingleValueResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


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

		// Secondary cursor should be initialized, but it doesn't support
		// putting data. So put some data into primary database before
		// starting up the secondary cursor
		TestUtils.putKeyValuePair(environment, database, 1, 11);
		startupCursor();
		final BJETxCursor fakeCursor = PowerMock
				.createStrictMock(BJETxCursor.class);
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
		final BJETxCursor fakeCursor = PowerMock
				.createStrictMock(BJETxCursor.class);
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
