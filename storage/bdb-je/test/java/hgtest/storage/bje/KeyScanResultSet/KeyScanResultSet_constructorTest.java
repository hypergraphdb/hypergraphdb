package hgtest.storage.bje.KeyScanResultSet;

import com.sleepycat.je.*;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSet_constructorTest extends KeyScanResultSetTestBasis
{
	private final DatabaseEntry keyIndex = new DatabaseEntry(new byte[] { 0, 0,
			0, 0 });

	@Test
	public void cursorIsNull() throws Exception
	{
		final Exception expected = new HGException(
				"java.lang.NullPointerException");

		PowerMock.replayAll();

		try
		{
			new KeyScanResultSet<Integer>(null, keyIndex, converter);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void keyIndexIsNull() throws Exception
	{
		startupCursor();
		TestUtils.putKeyValuePair(realCursor, 1, "one");
		createMocksForTheConstructor();
		PowerMock.replayAll();

		new KeyScanResultSet<Integer>(fakeCursor, null, converter);

		shutdownCursor();
	}

	@Test
	public void bjeCursorThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"java.lang.IllegalStateException: This exception is thrown by fake cursor.");

		final BJETxCursor fakeCursor = PowerMock.createMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andThrow(
				new IllegalStateException(
						"This exception is thrown by fake cursor."));
		PowerMock.replayAll();

		try
		{
			new KeyScanResultSet<Integer>(fakeCursor, keyIndex, converter);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void allIsOk() throws Exception
	{
		startupCursor();
		TestUtils.putKeyValuePair(realCursor, 1, "one");
		createMocksForTheConstructor();
		PowerMock.replayAll();

		new KeyScanResultSet<Integer>(fakeCursor, keyIndex, converter);

		shutdownCursor();
	}
}
