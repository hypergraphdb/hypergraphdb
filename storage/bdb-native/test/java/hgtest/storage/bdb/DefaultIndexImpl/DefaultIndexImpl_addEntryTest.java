package hgtest.storage.bdb.DefaultIndexImpl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bdb.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_addEntryTest extends DefaultIndexImplTestBasis
{
	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new HGException(
				"Attempting to operate on index 'sample_index' while the index is being closed.");

		PowerMock.replayAll();
		index = new DefaultIndexImpl(INDEX_NAME, storage, transactionManager,
				keyConverter, valueConverter, comparator);

		try
		{
			index.addEntry(1, "one");
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void keyIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		startupIndex();

		try
		{
			index.addEntry(null, "key is null");
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		closeDatabase(index);
	}

	@Test
	public void valueIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		startupIndex();

		try
		{
			index.addEntry(2, null);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		closeDatabase(index);
	}

	@Test
	public void transactionManagerThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to add entry to index 'sample_index': java.lang.IllegalStateException: This exception is thrown by fake transaction manager.");

		startupIndexWithFakeTransactionManager();

		try
		{
			index.addEntry(2, "two");
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
