package hgtest.storage.bje.KeyScanResultSet;

import org.easymock.EasyMock;
import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.powermock.api.easymock.PowerMock;

/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSet_goToTestBasis extends KeyScanResultSetTestBasis
{
	protected KeyScanResultSet<Integer> keyScan;

	protected void startupCursor()
	{
		realCursor = database.openCursor(transactionForTheEnvironment, null);
	}

	protected void createMocksForTheGoTo()
	{
		createMocksForTheConstructor();
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
		PowerMock.replayAll();
		keyScan = new KeyScanResultSet<Integer>(fakeCursor, null, converter);
	}

	protected void shutdownCursor()
	{
		realCursor.close();
	}
}
