package hgtest.storage.bje.SingleValueResultSet;

import com.sleepycat.je.DatabaseEntry;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.SingleValueResultSet;
import org.powermock.api.easymock.PowerMock;
import org.junit.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

public class SingleValueResultSet_goToExceptionsTest extends
		SingleValueResultSet_goToTestBasis
{
	@Test
	public void throwsException_whenValueIsNull() throws Exception
	{
		// Secondary cursor should be initialized, but it doesn't support
		// putting data. So put some data into primary database before
		// starting up the secondary cursor
		TestUtils.putKeyValuePair(environment, database, 1, 11);
		startupCursor();
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		expect(fakeCursor.cursor()).andReturn(realCursor);
		replay(fakeCursor);
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 0, 0, 0, 0 });
		final SingleValueResultSet<Integer> resultSet = new SingleValueResultSet<>(
				fakeCursor, key, converter);

		try
		{
			below.expect(NullPointerException.class);
			resultSet.goTo(null, true);
		}
		finally
		{
			shutdownCursor();
		}
	}

	@Test
	public void wrapsUnderlyingException_whenBjeCursorThrowsException()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 11);
		startupCursor();
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		expect(fakeCursor.cursor()).andReturn(realCursor);
		expect(fakeCursor.cursor()).andThrow(
				new IllegalStateException(
						"This exception is throws by fake BJE cursor."));
		replay(fakeCursor);
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 0, 0, 0, 0 });
		final SingleValueResultSet<Integer> resultSet = new SingleValueResultSet<>(
				fakeCursor, key, converter);

		try
		{
			below.expect(HGException.class);
			below.expectMessage("java.lang.IllegalStateException: This exception is throws by fake BJE cursor.");
			resultSet.goTo(1, true);
		}
		finally
		{
			shutdownCursor();
		}
	}
}
