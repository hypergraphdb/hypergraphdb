package hgtest.storage.bje.DefaultIndexImpl;

import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static hgtest.storage.bje.TestUtils.list;
import static org.testng.Assert.assertEquals;

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

		PowerMock.replayAll();
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, comparator);

		try
		{
			index.scanValues();
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
		}
	}

	@Test
	public void thereAreNotAddedEntries() throws Exception
	{
		final List<String> expected = Collections.emptyList();

		startupIndex();
		PowerMock.replayAll();

		final HGRandomAccessResult<String> result = index.scanValues();
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
	}

	@Test
	public void thereIsOneEntryAdded() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("first");

		startupIndex();
		PowerMock.replayAll();
		index.addEntry(1, "first");

		final HGRandomAccessResult<String> result = index.scanValues();
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesAdded() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("first");
		expected.add("second");
		expected.add("third");

		startupIndex();
		PowerMock.replayAll();
		index.addEntry(1, "first");
		index.addEntry(2, "second");
		index.addEntry(3, "third");

		final HGRandomAccessResult<String> result = index.scanValues();
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
	}

	@Test
	public void transactionManagerThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to lookup index 'sample_index': java.lang.IllegalStateException: This exception is thrown by fake transaction manager.");

		mockStorage();
		final HGTransactionManager fakeTransactionManager = PowerMock
				.createStrictMock(HGTransactionManager.class);
		EasyMock.expect(fakeTransactionManager.getContext())
				.andThrow(
						new IllegalStateException(
								"This exception is thrown by fake transaction manager."));
		PowerMock.replayAll();
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<Integer, String>(
				INDEX_NAME, storage, fakeTransactionManager, keyConverter,
				valueConverter, comparator);
		index.open();

		try
		{
			index.scanValues();
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
		}
		finally
		{
			index.close();
		}
	}
}
