package hgtest.storage.bje.SingleValueResultSet;

import com.sleepycat.je.*;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.SingleValueResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class SingleValueResultSet_constructorTest extends
		SingleValueResultSetTestBasis
{
	@Test
	public void bjeCursorIsNull() throws Exception
	{
		final Exception expected = new HGException(
				"java.lang.NullPointerException");

		final DatabaseEntry key = new DatabaseEntry(new byte[] { 1, 2, 3, 4 });
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

		try
		{
			new SingleValueResultSet(null, key, converter);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void keyIsNull() throws Exception
	{
		// put some data into primary database
		final Transaction transactionForAddingTestData = environment
				.beginTransaction(null, null);
		database.put(transactionForTheEnvironment, new DatabaseEntry(
				new byte[] { 0, 0, 0, 1 }), new DatabaseEntry(new byte[] { 0,
				0, 0, 2 }));
		transactionForAddingTestData.commit();
		final SecondaryCursor realCursor = secondaryDatabase.openCursor(
				transactionForTheEnvironment, null);
		final DatabaseEntry stubKey = new DatabaseEntry();
		final DatabaseEntry stubValue = new DatabaseEntry();
		// initialize secondary cursor
		realCursor.getFirst(stubKey, stubValue, LockMode.DEFAULT);
		final BJETxCursor fakeCursor = PowerMock
				.createStrictMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
		PowerMock.replayAll();
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

		new SingleValueResultSet(fakeCursor, null, converter);

		realCursor.close();
	}

	@Test
	public void converterIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		final BJETxCursor fakeCursor = PowerMock
				.createStrictMock(BJETxCursor.class);
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry();

		try
		{
			new SingleValueResultSet(fakeCursor, key, null);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void bjeCursorThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"java.lang.IllegalStateException: This exceptions is thrown by fake cursor.");

		final BJETxCursor fakeCursor = PowerMock
				.createStrictMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andThrow(
				new IllegalStateException(
						"This exceptions is thrown by fake cursor."));
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 1, 2, 3, 4 });
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

		try
		{
			new SingleValueResultSet(fakeCursor, key, converter);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void allIsOk() throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 2, 4);
		startupCursor();
		createMocksForTheConstructor();
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 1, 2, 3, 4 });

		new SingleValueResultSet(fakeCursor, key, converter);

		shutdownCursor();
	}
}
