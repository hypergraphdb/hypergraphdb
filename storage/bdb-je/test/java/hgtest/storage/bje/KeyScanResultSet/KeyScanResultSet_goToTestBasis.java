package hgtest.storage.bje.KeyScanResultSet;

import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.replayAll;

import org.hypergraphdb.storage.bje.KeyScanResultSet;

public abstract class KeyScanResultSet_goToTestBasis extends KeyScanResultSetTestBasis
{
	protected KeyScanResultSet<Integer> keyScan;

	protected void startupCursor()
	{
		realCursor = database.openCursor(transactionForTheEnvironment, null);
	}

	protected void createMocksForTheGoTo()
	{
		createMocksForTheConstructor();
		expect(fakeCursor.cursor()).andReturn(realCursor);
		replayAll();
		keyScan = new KeyScanResultSet<>(fakeCursor, null, converter);
	}

	protected void shutdownCursor()
	{
		realCursor.close();
	}
}
