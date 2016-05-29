package hgtest.storage.bje.KeyScanResultSet;

import static hgtest.storage.bje.TestUtils.putKeyValuePair;
import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.createStrictMock;
import static org.powermock.api.easymock.PowerMock.replayAll;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.junit.Test;

public class KeyScanResultSet_goToExceptionsTest extends
		KeyScanResultSet_goToTestBasis
{
	@Test
	public void wrapsUnderlyingException_withHypergraphException()
			throws Exception
	{
		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		// one call with real cursor for constructor
		expect(fakeCursor.cursor()).andReturn(realCursor);
		// another one call for the goTo() method
		expect(fakeCursor.cursor()).andThrow(
				new IllegalStateException(
						"This exception is thrown by fake BJE cursor."));
		replayAll();
		final KeyScanResultSet<Integer> keyScan = new KeyScanResultSet<>(
				fakeCursor, null, converter);

		try
		{
			below.expect(HGException.class);
			below.expectMessage("java.lang.IllegalStateException: This exception is thrown by fake BJE cursor.");
			keyScan.goTo(1, true);
		}
		finally
		{
			shutdownCursor();
		}
	}
}
