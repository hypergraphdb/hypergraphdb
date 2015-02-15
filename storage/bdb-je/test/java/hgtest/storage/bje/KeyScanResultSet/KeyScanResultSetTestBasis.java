package hgtest.storage.bje.KeyScanResultSet;

import com.sleepycat.je.Cursor;
import hgtest.storage.bje.ResultSetTestBasis;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.powermock.api.easymock.PowerMock;

/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSetTestBasis extends ResultSetTestBasis
{
	protected Cursor realCursor;

	protected BJETxCursor fakeCursor;
	protected final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

	protected void startupCursor()
	{
		realCursor = database.openCursor(transactionForTheEnvironment, null);
	}

	protected void shutdownCursor()
	{
		realCursor.close();
	}

	protected void createMocksForTheConstructor()
	{
		fakeCursor = PowerMock.createMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
	}
}
