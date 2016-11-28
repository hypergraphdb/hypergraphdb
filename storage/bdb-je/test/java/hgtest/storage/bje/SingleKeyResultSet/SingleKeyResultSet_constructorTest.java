package hgtest.storage.bje.SingleKeyResultSet;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.SingleKeyResultSet;
import org.junit.Test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;

import hgtest.storage.bje.ResultSetTestBasis;
import hgtest.storage.bje.TestUtils;

public class SingleKeyResultSet_constructorTest extends ResultSetTestBasis
{
	private final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
	private final DatabaseEntry key = new DatabaseEntry(
			new byte[] { 0, 0, 0, 0 });

	@Test
	public void throwsException_whenBjeCursorIsNull() throws Exception
	{
		below.expect(HGException.class);
		new SingleKeyResultSet<>(null, key, converter);
	}

	@Test
	public void doesNotFail_whenKeyIsNull() throws Exception
	{
		final Cursor realCursor = database.openCursor(
				transactionForTheEnvironment, null);
		// initialize cursor
		realCursor.put(new DatabaseEntry(new byte[] { 1, 2, 3, 4 }),
				new DatabaseEntry(new byte[] { 1, 2, 3, 4 }));
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		expect(fakeCursor.cursor()).andReturn(realCursor).times(4);
		replay(fakeCursor);

		new SingleKeyResultSet<>(fakeCursor, null, converter);

		realCursor.close();
	}

	@Test
	public void throwsException_whenConverterIsNull() throws Exception
	{
		final Cursor realCursor = database.openCursor(
				transactionForTheEnvironment, null);
		realCursor.put(new DatabaseEntry(new byte[] { 1, 2, 3, 4 }),
				new DatabaseEntry(new byte[] { 1, 2, 3, 4 }));
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		expect(fakeCursor.cursor()).andReturn(realCursor).times(4);
		replay(fakeCursor);

		try
		{
			below.expect(HGException.class);
			below.expectMessage("java.lang.NullPointerException");
			new SingleKeyResultSet<>(fakeCursor, key, null);
		}
		finally
		{
			realCursor.close();
		}
	}

	@Test
	public void doesNotWrapUnderlyingException_whenFakeCursorThrowsException()
			throws Exception
	{
		final Cursor realCursor = database.openCursor(
				transactionForTheEnvironment, null);
		realCursor.put(new DatabaseEntry(new byte[] { 1, 2, 3, 4 }),
				new DatabaseEntry(new byte[] { 1, 2, 3, 4 }));
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		// at the first call return real cursor
		expect(fakeCursor.cursor()).andReturn(realCursor);
		// at the second call throw exception
		expect(fakeCursor.cursor()).andThrow(
				new IllegalStateException(
						"This exception is thrown by fake cursor."));
		replay(fakeCursor);

		try
		{
			below.expect(HGException.class);
			below.expectMessage("This exception is thrown by fake cursor.");
			new SingleKeyResultSet<>(fakeCursor, key, converter);
		}
		finally
		{
			realCursor.close();
		}
	}

	@Test
	public void happyPath() throws Exception
	{
		final Cursor realCursor = database.openCursor(
				transactionForTheEnvironment, null);
		realCursor.put(new DatabaseEntry(new byte[] { 1, 2, 3, 4 }),
				new DatabaseEntry(new byte[] { 1, 2, 3, 4 }));
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		expect(fakeCursor.cursor()).andReturn(realCursor).times(4);
		replay(fakeCursor);

		new SingleKeyResultSet<>(fakeCursor, key, converter);

		realCursor.close();
	}
}
