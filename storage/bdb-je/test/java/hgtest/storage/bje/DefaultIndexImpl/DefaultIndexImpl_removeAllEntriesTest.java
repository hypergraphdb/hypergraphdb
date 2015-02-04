package hgtest.storage.bje.DefaultIndexImpl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_removeAllEntriesTest extends
		DefaultIndexImplTestBasis
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
			index.removeAllEntries(0);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void thereAreNotAddedEntries() throws Exception
	{
		startupIndex();

		index.removeAllEntries(0);

		index.close();
	}

	@Test
	public void keyIsNull() throws Exception
	{
		startupIndex();

		try
		{
			index.removeAllEntries(null);
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
	public void thereIsOneAddedEntry() throws Exception
	{
		startupIndex();
		index.addEntry(1, "one");

		index.removeAllEntries(1);

		final String data = index.getData(1);
		assertNull(data);
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesButDesiredEntryDoesNotExist()
			throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("one");
		expected.add("two");

		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(2, "two");

		index.removeAllEntries(5);

		final List<String> actual = new ArrayList<String>();
		actual.add(index.getData(1));
		actual.add(index.getData(2));
		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesAndDesiredEntryExists() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("one");
		expected.add(null);
		expected.add("three");

		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		index.removeAllEntries(2);

		final List<String> actual = new ArrayList<String>();
		actual.add(index.getData(1));
		actual.add(index.getData(2));
		actual.add(index.getData(3));
		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereAreEntriesWithTheSameKey() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("one");
		expected.add("two");
		expected.add(null);

		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");
		index.addEntry(3, "third");

		index.removeAllEntries(3);

		final List<String> actual = new ArrayList<String>();
		actual.add(index.getData(1));
		actual.add(index.getData(2));
		actual.add(index.getData(3));
		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void transactionManagerThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to delete entry from index 'sample_index': java.lang.IllegalStateException: This exception is thrown by fake transaction manager.");

		startupIndexWithFakeTransactionManager();

		try
		{
			index.removeAllEntries(1);
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
