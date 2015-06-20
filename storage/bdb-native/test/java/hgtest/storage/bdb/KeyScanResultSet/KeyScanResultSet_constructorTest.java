package hgtest.storage.bdb.KeyScanResultSet;

import com.sleepycat.db.*;
import hgtest.storage.bdb.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bdb.BDBTxCursor;
import org.hypergraphdb.storage.bdb.KeyScanResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


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

		final BDBTxCursor fakeCursor = PowerMock.createMock(BDBTxCursor.class);
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
