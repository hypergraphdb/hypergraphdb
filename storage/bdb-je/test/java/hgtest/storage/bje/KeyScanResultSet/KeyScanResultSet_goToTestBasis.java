package hgtest.storage.bje.KeyScanResultSet;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;
import hgtest.storage.bje.ResultSetTestBasis;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.powermock.api.easymock.PowerMock;

/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSet_goToTestBasis extends ResultSetTestBasis {
    protected Cursor realCursor;
    protected Transaction transactionForTheRealCursor;

    protected final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
    protected KeyScanResultSet<Integer> keyScan;

    protected void startupCursor() throws Exception
    {
        transactionForTheRealCursor = environment.beginTransaction(null, null);
        realCursor = database.openCursor(transactionForTheEnvironment, null);
    }

    protected void startupMocks()
    {
        final BJETxCursor fakeCursor = PowerMock.createMock(BJETxCursor.class);
        EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor).times(2);
        PowerMock.replayAll();
        keyScan = new KeyScanResultSet<Integer>(fakeCursor, null, converter);
    }

    protected void shutdownCursor()
    {
        realCursor.close();
        transactionForTheRealCursor.commit();
    }
}
