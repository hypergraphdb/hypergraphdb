package hgtest.storage.bje.DefaultIndexImpl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static hgtest.TestUtils.assertExceptions;
import static hgtest.TestUtils.list;
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
	public void thereAreNotAddedEntries() throws Exception
	{
		final List<String> expected = Collections.emptyList();

		startupIndex();

		final HGSearchResult<String> result = index.findLT(2);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
	}

	@Test
	public void thereIsOneEntryAddedButItIsLessThanDesired() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("A");

		startupIndex();
		index.addEntry(2, "A");

		final HGSearchResult<String> result = index.findLT(3);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
	}

	@Test
	public void thereIsOneEntryAddedButItIsGreaterThanDesired()
			throws Exception
	{
		final Exception expected = new HGException(
				"Failed to lookup index 'sample_index': java.lang.NullPointerException");

		startupIndex();
		index.addEntry(4, "A");

		try
		{
			index.findLT(3);
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

	@Test
	public void thereIsOneEntryAddedButItIsEqualToTheDesired() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to lookup index 'sample_index': java.lang.NullPointerException");

		startupIndex();
		index.addEntry(3, "A");

		try
		{
			index.findLT(3);
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

	@Test
	public void thereAreSeveralEntriesAddedButAllOfThemAreLessThanDesired()
			throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("B");
		expected.add("A");

		startupIndex();
		index.addEntry(3, "B");
		index.addEntry(2, "A");

		final HGSearchResult<String> result = index.findLT(4);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesAddedButAllOfThemAreGreaterThanDesired()
			throws Exception
	{
		final Exception expected = new HGException(
				"Failed to lookup index 'sample_index': java.lang.NullPointerException");

		startupIndex();
		index.addEntry(2, "A");
		index.addEntry(3, "B");

		try
		{
			index.findLT(1);
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

	@Test
	public void thereAreSeveralEntriesAddedButAllOfThemAreEqualToDesired()
			throws Exception
	{
		final Exception expected = new HGException(
				"Failed to lookup index 'sample_index': java.lang.NullPointerException");

		startupIndex();
		index.addEntry(3, "A");
		index.addEntry(3, "B");

		try
		{
			index.findLT(3);
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

	@Test
	public void thereAreSeveralEntriesAdded() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("D");
		expected.add("C");
		expected.add("B");
		expected.add("A");

		startupIndex();
		index.addEntry(4, "D");
		index.addEntry(2, "B");
		index.addEntry(1, "A");
		index.addEntry(3, "C");

		final HGSearchResult<String> result = index.findLT(5);
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
