package hgtest.storage.bje.DefaultBiIndexImpl;

import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static hgtest.storage.bje.TestUtils.list;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_findByValue extends
		DefaultBiIndexImpl_TestBasis
{
	@Test
	public void thereIsOneEntry() throws Exception
	{
		final List<Integer> expected = new ArrayList<Integer>();
		expected.add(1);

		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, null);
		indexImpl.open();
		indexImpl.addEntry(1, "one");

		final HGRandomAccessResult<Integer> result = indexImpl
				.findByValue("one");

		final List<Integer> actual = list(result);
		assertEquals(actual, expected);
		result.close();
		indexImpl.close();
	}

	@Test
	public void thereAreNotAddedEntries() throws Exception
	{
		final List<Integer> expected = Collections.emptyList();

		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, null);
		indexImpl.open();

		final HGRandomAccessResult<Integer> result = indexImpl
				.findByValue("this value doesn't exist");

		final List<Integer> actual = list(result);
		assertEquals(actual, expected);
		result.close();
		indexImpl.close();
	}

	@Test
	public void thereAreDuplicatedValues() throws Exception
	{
		final List<Integer> expected = new ArrayList<Integer>();
		expected.add(2);
		expected.add(3);

		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, null);
		indexImpl.open();
		indexImpl.addEntry(2, "word");
		indexImpl.addEntry(3, "word");

		final HGRandomAccessResult<Integer> result = indexImpl
				.findByValue("word");
		final List<Integer> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		indexImpl.close();
	}

	@Test
	public void thereSeveralValuesSomeOfThemAreDuplicated() throws Exception
	{
		final List<Integer> expected = new ArrayList<Integer>();
		expected.add(2);

		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, null);
		indexImpl.open();
		indexImpl.addEntry(0, "red");
		indexImpl.addEntry(1, "orange");
		indexImpl.addEntry(2, "yellow");
		indexImpl.addEntry(11, "orange");

		final HGRandomAccessResult<Integer> result = indexImpl
				.findByValue("yellow");
		final List<Integer> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		indexImpl.close();
	}

	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new HGException(
				"Attempting to lookup index 'sample_index' while it is closed.");

		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, null);

		try
		{
			indexImpl.findByValue("some value");
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
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
				"Failed to lookup index 'sample_index': java.lang.IllegalStateException");

		mockStorage();
		final HGTransactionManager fakeTransactionManager = PowerMock
				.createStrictMock(HGTransactionManager.class);
		EasyMock.expect(fakeTransactionManager.getContext()).andThrow(
				new IllegalStateException());
		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, null);
		indexImpl.open();
		indexImpl.addEntry(0, "red");

		// inject fake transaction manager
		final Field transactionManagerField = indexImpl.getClass().getSuperclass().getDeclaredField("transactionManager");
		transactionManagerField.setAccessible(true);
		transactionManagerField.set(indexImpl, fakeTransactionManager);
		try
		{
			indexImpl
					.findByValue("yellow");
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
		}
		finally
		{
			indexImpl.close();
		}
	}
}
