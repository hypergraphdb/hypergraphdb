package hgtest.storage.bje.SingleKeyResultSet;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import hgtest.storage.bje.ResultSetTestBasis;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.SingleKeyResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class SingleKeyResultSet_constructorTest extends ResultSetTestBasis {
    @Test
    public void bjeCursorIsNull() throws Exception {
        final Exception expected = new HGException(
                "java.lang.NullPointerException");

        final DatabaseEntry key = new DatabaseEntry(new byte[]{0, 0, 0, 0});
        final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

        try {
            new SingleKeyResultSet(null, key, converter);
        } catch (Exception occurred) {
            assertEquals(occurred.getClass(), expected.getClass());
            assertEquals(occurred.getMessage(), expected.getMessage());
        }
    }

    @Test
    public void keyIsNull() throws Exception {
        final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
        final Cursor realCursor = database.openCursor(transactionForTheEnvironment, null);
        // initialize cursor
        realCursor.put(new DatabaseEntry(new byte[]{1, 2, 3, 4}), new DatabaseEntry(new byte[]{1, 2, 3, 4}));
        final BJETxCursor fakeCursor = PowerMock.createMock(BJETxCursor.class);
        EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor).times(2);
        PowerMock.replayAll();

        new SingleKeyResultSet(fakeCursor, null, converter);

        realCursor.close();
    }

    @Test
    public void converterIsNull() throws Exception {
        final Exception expected = new HGException(
                "java.lang.NullPointerException");

        final Cursor realCursor = database.openCursor(transactionForTheEnvironment, null);
        realCursor.put(new DatabaseEntry(new byte[]{1, 2, 3, 4}), new DatabaseEntry(new byte[]{1, 2, 3, 4}));
        final BJETxCursor fakeCursor = PowerMock.createMock(BJETxCursor.class);
        EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
        PowerMock.replayAll();
        final DatabaseEntry key = new DatabaseEntry(new byte[]{0, 0, 0, 0});

        try {
            new SingleKeyResultSet(fakeCursor, key, null);
        } catch (Exception occurred) {
            assertEquals(occurred.getClass(), expected.getClass());
            assertEquals(occurred.getMessage(), expected.getMessage());
        } finally {
            realCursor.close();
        }
    }

    @Test
    public void fakeCursorThrowsException() throws Exception {
        final Exception expected = new HGException(
                "java.lang.IllegalStateException: This exception is thrown by fake cursor.");

        final Cursor realCursor = database.openCursor(transactionForTheEnvironment, null);
        realCursor.put(new DatabaseEntry(new byte[]{1, 2, 3, 4}), new DatabaseEntry(new byte[]{1, 2, 3, 4}));
        final BJETxCursor fakeCursor = PowerMock.createMock(BJETxCursor.class);
        // at the first call return real cursor
        EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
        // at the second call throw exception
        EasyMock.expect(fakeCursor.cursor()).andThrow(new IllegalStateException("This exception is thrown by fake cursor."));
        PowerMock.replayAll();
        final DatabaseEntry key = new DatabaseEntry(new byte[]{0, 0, 0, 0});
        final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

        try {
            new SingleKeyResultSet(fakeCursor, key, converter);
        } catch (Exception occurred) {
            assertEquals(occurred.getClass(), expected.getClass());
            assertEquals(occurred.getMessage(), expected.getMessage());
        } finally {
            realCursor.close();
        }
    }
}
