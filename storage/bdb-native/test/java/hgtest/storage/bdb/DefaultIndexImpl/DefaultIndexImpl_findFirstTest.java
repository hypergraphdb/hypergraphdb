package hgtest.storage.bdb.DefaultIndexImpl;

import com.google.code.multitester.annonations.Exported;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bdb.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_findFirstTest extends DefaultIndexImplTestBasis
{
	@Exported("up3")
	protected void replayMocks()
	{
		PowerMock.replayAll();
	}

	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new HGException(
				"Attempting to operate on index 'sample_index' while the index is being closed.");

		replayMocks();
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, comparator);

		try
		{
			index.findFirst(0);
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
		replayMocks();

		try
		{
			index.findFirst(null);
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
			index.findFirst(2);
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
