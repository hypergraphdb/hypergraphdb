package hgtest.storage.bdb.KeyScanResultSet;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseException;
import hgtest.storage.bdb.ResultSetTestBasis;
import hgtest.storage.bdb.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bdb.BDBTxCursor;
import org.powermock.api.easymock.PowerMock;

/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSetTestBasis extends ResultSetTestBasis
{
	protected Cursor realCursor;

	protected BDBTxCursor fakeCursor;
	protected final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

	protected void startupCursor() throws DatabaseException
	{
		realCursor = database.openCursor(transactionForTheEnvironment, null);
	}

	protected void shutdownCursor() throws DatabaseException
	{
		realCursor.close();
	}

	protected void createMocksForTheConstructor()
	{
		fakeCursor = PowerMock.createMock(BDBTxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
	}
}
