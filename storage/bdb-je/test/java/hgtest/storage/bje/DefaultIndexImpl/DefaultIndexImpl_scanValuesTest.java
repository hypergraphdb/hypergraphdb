package hgtest.storage.bje.DefaultIndexImpl;


import com.google.code.multitester.annonations.Exported;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.junit.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.easymock.EasyMock.replay;


/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_scanValuesTest extends DefaultIndexImplTestBasis
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
			index.scanValues();
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
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
			index.scanValues();
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
