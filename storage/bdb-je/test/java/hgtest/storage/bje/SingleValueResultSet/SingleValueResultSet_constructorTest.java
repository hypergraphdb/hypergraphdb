package hgtest.storage.bje.SingleValueResultSet;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.SingleValueResultSet;
import org.junit.Test;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.Transaction;
import hgtest.storage.bje.TestUtils;

public class SingleValueResultSet_constructorTest extends
		SingleValueResultSetTestBasis
{
	@Test
	public void throwsException_whenBjeCursorIsNull() throws Exception
	{
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 1, 2, 3, 4 });
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

		below.expect(HGException.class);
		below.expectMessage("java.lang.NullPointerException");
		new SingleValueResultSet(null, key, converter);
	}

	@Test
	public void throwsException_whenKeyIsNull() throws Exception
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
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		expect(fakeCursor.cursor()).andReturn(realCursor);
		replay(fakeCursor);
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

		new SingleValueResultSet<>(fakeCursor, null, converter);

		realCursor.close();
	}

	@Test
	public void throwsException_whenConverterIsNull() throws Exception
	{
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		replay(fakeCursor);
		final DatabaseEntry key = new DatabaseEntry();

		below.expect(NullPointerException.class);
		new SingleValueResultSet<>(fakeCursor, key, null);
	}

	@Test
	public void bjeCursorThrowsException() throws Exception
	{
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		expect(fakeCursor.cursor()).andThrow(
				new IllegalStateException(
						"This exceptions is thrown by fake cursor."));
		replay(fakeCursor);
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 1, 2, 3, 4 });
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

		below.expect(HGException.class);
		below.expectMessage("java.lang.IllegalStateException: This exceptions is thrown by fake cursor.");
		new SingleValueResultSet<>(fakeCursor, key, converter);
	}

	@Test
	public void happyPath() throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 2, 4);
		startupCursor();
		createMocksForTheConstructor();
		replay(fakeCursor);
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 1, 2, 3, 4 });

		new SingleValueResultSet<>(fakeCursor, key, converter);

		shutdownCursor();
	}
}
