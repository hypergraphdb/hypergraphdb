package hgtest.storage.bje.DefaultIndexImpl;

import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
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
public class DefaultIndexImpl_findTest extends DefaultIndexImplTestBasis
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
			index.find(5);
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
		}
	}

	@Test
	public void keyIsNull() throws Exception
	{
		startupIndex();

		try
		{
			index.find(null);
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

		final HGRandomAccessResult<String> result = index.find(1);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
	}

	@Test
	public void thereIsOneAddedEntry() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("A");

		startupIndex();
		index.addEntry(65, "A");

		final HGRandomAccessResult<String> result = index.find(65);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesButDesiredEntryDoesNotExist()
			throws Exception
	{
		final List<String> expected = Collections.emptyList();

		startupIndex();
		index.addEntry(65, "A");
		index.addEntry(66, "B");

		final HGRandomAccessResult<String> result = index.find(67);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesAndDesiredEntryExists() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("C");

		startupIndex();
		index.addEntry(65, "A");
		index.addEntry(66, "B");
		index.addEntry(67, "C");

		final HGRandomAccessResult<String> result = index.find(67);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
	}

	@Test
	public void thereAreEntriesWithTheSameKey() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
        expected.add("ASCII 'B' letter");
        expected.add("B");

		startupIndex();
		index.addEntry(65, "A");
		index.addEntry(66, "B");
		index.addEntry(67, "C");
		index.addEntry(66, "ASCII 'B' letter");

		final HGRandomAccessResult<String> result = index.find(66);
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
            index.find(2);
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
