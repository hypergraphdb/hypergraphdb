package hgtest.storage.bje.KeyScanResultSet;

import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSet_goToExceptionsTest extends
		KeyScanResultSet_goToTestBasis
{
	@Test
	public void bjeCursorThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"java.lang.IllegalStateException: This exception is thrown by fake BJE cursor.");

		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		final BJETxCursor fakeCursor = PowerMock.createMock(BJETxCursor.class);
		// one call with real cursor for constructor
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
		// another one call for the goTo() method
		EasyMock.expect(fakeCursor.cursor()).andThrow(
				new IllegalStateException(
						"This exception is thrown by fake BJE cursor."));
		PowerMock.replayAll();
		final KeyScanResultSet<Integer> keyScan = new KeyScanResultSet<Integer>(
				fakeCursor, null, converter);

		try
		{
			keyScan.goTo(1, true);
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
		}
		finally
		{
			shutdownCursor();
		}
	}
}
