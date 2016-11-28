package hgtest.storage.bje.KeyScanResultSet;

import static hgtest.storage.bje.TestUtils.putKeyValuePair;
import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.createStrictMock;
import static org.powermock.api.easymock.PowerMock.replayAll;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.junit.Test;

import com.sleepycat.je.DatabaseEntry;

public class KeyScanResultSet_constructorTest extends KeyScanResultSetTestBasis
{
	protected final DatabaseEntry sampleKeyIndex = new DatabaseEntry(new byte[] { 0,
			0, 0, 0 });

	@Test
	public void throwsException_whenCursorIsNull() throws Exception
	{
		replayAll();

		below.expect(HGException.class);
		below.expectMessage("java.lang.NullPointerException");
		new KeyScanResultSet<>(null, sampleKeyIndex, converter);
	}

	@Test
	public void doesNotFail_whenKeyIndexIsNull() throws Exception
	{
		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		createMocksForTheConstructor();
		replayAll();

		new KeyScanResultSet<>(fakeCursor, null, converter);

		shutdownCursor();
	}

	@Test
	public void wrapsUnderlyingException_withHypergraphException()
			throws Exception
	{
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		expect(fakeCursor.cursor()).andThrow(
				new IllegalStateException(
						"This exception is thrown by fake cursor."));
		replayAll();

		below.expect(HGException.class);
		below.expectMessage("java.lang.IllegalStateException: This exception is thrown by fake cursor.");
		new KeyScanResultSet<>(fakeCursor, sampleKeyIndex, converter);
	}

	@Test
	public void happyPath() throws Exception
	{
		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		createMocksForTheConstructor();
		replayAll();

		new KeyScanResultSet<>(fakeCursor, sampleKeyIndex, converter);

		shutdownCursor();
	}
}
