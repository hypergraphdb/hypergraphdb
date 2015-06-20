package hgtest.storage.bdb.DefaultIndexImpl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bdb.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_findLTTest extends DefaultIndexImplTestBasis
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
            index.findLT(1);
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

        try
        {
            index.findLT(null);
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
    public void transactionManagerThrowsException() throws Exception
    {
        final Exception expected = new HGException(
                "Failed to lookup index 'sample_index': java.lang.IllegalStateException: This exception is thrown by fake transaction manager.");

        startupIndexWithFakeTransactionManager();

        try
        {
            index.findLT(2);
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
