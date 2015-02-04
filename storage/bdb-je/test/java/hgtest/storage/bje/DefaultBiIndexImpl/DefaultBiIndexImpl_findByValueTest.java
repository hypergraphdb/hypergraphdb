package hgtest.storage.bje.DefaultBiIndexImpl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static hgtest.storage.bje.TestUtils.list;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_findByValueTest extends
		DefaultBiIndexImplTestBasis
{
	private HGRandomAccessResult<Integer> result;

	private void closeResultAndIndex()
	{
		result.close();
		indexImpl.close();
	}

	@Test
	public void findByNullValue() throws Exception
	{
		final Exception expected = new NullPointerException();

		startupIndex();

		try
		{
			indexImpl.findByValue(null);
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
		}
		finally
		{
			indexImpl.close();
		}
	}

	@Test
	public void thereIsOneEntry() throws Exception
	{
		final List<Integer> expected = new ArrayList<Integer>();
		expected.add(1);

		startupIndex();
		indexImpl.addEntry(1, "one");

		result = indexImpl.findByValue("one");

		final List<Integer> actual = list(result);
		assertEquals(actual, expected);
		closeResultAndIndex();
	}

	@Test
	public void thereAreNotAddedEntries() throws Exception
	{
		final List<Integer> expected = Collections.emptyList();

		startupIndex();

		result = indexImpl.findByValue("this value doesn't exist");

		final List<Integer> actual = list(result);
		assertEquals(actual, expected);
		closeResultAndIndex();
	}

	@Test
	public void thereAreDuplicatedValues() throws Exception
	{
		final List<Integer> expected = new ArrayList<Integer>();
		expected.add(2);
		expected.add(3);

		startupIndex();
		indexImpl.addEntry(2, "word");
		indexImpl.addEntry(3, "word");

		result = indexImpl.findByValue("word");
		final List<Integer> actual = list(result);

		assertEquals(actual, expected);
		closeResultAndIndex();
	}

	@Test
	public void thereAreSeveralEntriesButDesiredValueDoesNotExist()
			throws Exception
	{
		final List<Integer> expected = Collections.emptyList();

		startupIndex();
		indexImpl.addEntry(2, "two");
		indexImpl.addEntry(3, "three");

		result = indexImpl.findByValue("none");
		final List<Integer> actual = list(result);

		assertEquals(actual, expected);
		closeResultAndIndex();
	}

	@Test
	public void thereSeveralValuesSomeOfThemAreDuplicated() throws Exception
	{
		final List<Integer> expected = new ArrayList<Integer>();
		expected.add(2);

		startupIndex();
		indexImpl.addEntry(0, "red");
		indexImpl.addEntry(1, "orange");
		indexImpl.addEntry(2, "yellow");
		indexImpl.addEntry(11, "orange");

		result = indexImpl.findByValue("yellow");
		final List<Integer> actual = list(result);

		assertEquals(actual, expected);
		closeResultAndIndex();
	}

	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new HGException(
				"Attempting to lookup index 'sample_index' while it is closed.");

		PowerMock.replayAll();
		indexImpl = new DefaultBiIndexImpl<Integer, String>(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);

		try
		{
			indexImpl.findByValue("some value");
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			indexImpl.close();
		}
	}

	@Test
	public void transactionManagerThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to lookup index 'sample_index': java.lang.IllegalStateException: Transaction manager is fake.");

		startupIndexWithFakeTransactionManager();

		try
		{
			indexImpl.findByValue("yellow");
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			indexImpl.close();
		}
	}
}
