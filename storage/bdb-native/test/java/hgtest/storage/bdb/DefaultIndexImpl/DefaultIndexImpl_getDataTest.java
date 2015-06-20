package hgtest.storage.bdb.DefaultIndexImpl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bdb.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_getDataTest extends DefaultIndexImplTestBasis
{
    @Test
    public void indexIsNotOpened() throws Exception
    {
        final Exception expected = new HGException(
                "Attempting to operate on index 'sample_index' while the index is being closed.");

        PowerMock.replayAll();
        final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<Integer, String>(
                INDEX_NAME, storage, transactionManager, keyConverter,
                valueConverter, comparator);

        try
        {
            index.getData(2);
        }
        catch (Exception occurred)
        {
            assertExceptions(occurred, expected);
        }
    }

    @Test
    public void keyIsNull() throws Exception
    {
        startupIndex();
        PowerMock.replayAll();

        try
        {
            index.getData(null);
        }
        catch (Exception occurred)
        {
            assertEquals(occurred.getClass(), NullPointerException.class);
        }
        finally
        {
            index.close();
        }
    }

    @Test
    public void thereAreNotAddedEntries() throws Exception
    {
        startupIndex();
        PowerMock.replayAll();

        final String data = index.getData(2);

        assertNull(data);
        index.close();
    }

    @Test
    public void thereAreSeveralEntriesAddedByDesiredEntryDoesNotExist()
            throws Exception
    {
        startupIndex();
        PowerMock.replayAll();
        index.addEntry(1, "first");
        index.addEntry(2, "second");

        final String data = index.getData(3);

        assertNull(data);
        index.close();
    }

    @Test
    public void thereAreSeveralEntriesAddedAndDesiredEntryExists()
            throws Exception
    {
        final String expected = "third";

        startupIndex();
        PowerMock.replayAll();
        index.addEntry(1, "first");
        index.addEntry(2, "second");
        index.addEntry(3, "third");

        final String actual = index.getData(3);

        assertEquals(actual, expected);
        index.close();
    }

    @Test
    public void transactionManagerThrowsException() throws Exception
    {
        final Exception expected = new HGException(
                "Failed to lookup index 'sample_index': java.lang.IllegalStateException: This exception is thrown by fake transaction manager.");

        startupIndexWithFakeTransactionManager();

        try
        {
            index.getData(22);
        }
        catch (Exception occurred)
        {
            assertExceptions(occurred, expected);
        }
        finally
        {
            index.close();
        }
    }
}
