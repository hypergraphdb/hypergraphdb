package hgtest.storage.bdb.KeyScanResultSet;

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
public class KeyScanResultSet_goToExceptionsTest extends
		KeyScanResultSet_goToTestBasis
{
    @Test
    public void bdbCursorThrowsException() throws Exception
    {
        final Exception expected = new HGException(
                "java.lang.IllegalStateException: This exception is thrown by fake BDB cursor.");

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        final BDBTxCursor fakeCursor = PowerMock.createMock(BDBTxCursor.class);
        EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
        EasyMock.expect(fakeCursor.cursor()).andThrow(
                new IllegalStateException(
                        "This exception is thrown by fake BDB cursor."));
        PowerMock.replayAll();
        final KeyScanResultSet<Integer> keyScan = new KeyScanResultSet<Integer>(
                fakeCursor, null, converter);

        try
        {
            keyScan.goTo(1, true);
        }
        catch (Exception occurred)
        {
            assertExceptions(occurred, expected);
        }
        finally
        {
            shutdownCursor();
        }
    }
}
