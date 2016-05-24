package hgtest.storage.bje.DefaultIndexImpl;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.junit.Test;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_findGTETest extends DefaultIndexImplTestBasis
{
	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new HGException(
				"Attempting to operate on index 'sample_index' while the index is being closed.");

        replay(mockedStorage);
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<Integer, String>(
				INDEX_NAME, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		try
		{
			index.findGTE(1);
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
			index.findGTE(null);
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
			index.findGTE(2);
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