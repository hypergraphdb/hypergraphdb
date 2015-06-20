package hgtest.storage.bje.DefaultIndexImpl;

import com.sleepycat.je.DatabaseNotFoundException;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_countByKeyTest extends DefaultIndexImplTestBasis
{
	@Test
	public void indexIsNopOpened() throws Exception
	{
		final Exception expected = new NullPointerException();

		PowerMock.replayAll();
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, comparator);

		try
		{
			index.count(1);
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
			index.count(null);
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
	public void thereAreNotAddedEntries() throws Exception
	{
		final long expected = 0;

		startupIndex();

		final long actual = index.count(1);

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereIsOneEntryAddedButItIsNotEqualToDesired() throws Exception
	{
		final long expected = 0;

		startupIndex();
		index.addEntry(1, "one");

		final long actual = index.count(2);

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereIsOneEntryAddedAndItIsEqualToDesired() throws Exception
	{
		final long expected = 1;

		startupIndex();
		index.addEntry(1, "one");

		final long actual = index.count(1);

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesAddedButThereAreNotDesired()
			throws Exception
	{
		final long expected = 0;

		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		final long actual = index.count(5);

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesAddedButThereAreNotDuplicatedKeys()
			throws Exception
	{
		final long expected = 1;

		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		final long actual = index.count(3);

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesAddedAndSomeKeysAreDuplicated()
			throws Exception
	{
		final long expected = 2;

		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");
		index.addEntry(3, "third");

		final long actual = index.count(3);

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void transactionManagerThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"This exception is thrown by fake transaction manager.");

		// create index and open it in usual way
		startupIndex();
		PowerMock.verifyAll();
		PowerMock.resetAll();

		// after index is opened inject fake transaction manager and call
		// count(KeyType key) method
		final HGTransactionManager fakeTransactionManager = PowerMock
				.createStrictMock(HGTransactionManager.class);
		EasyMock.expect(fakeTransactionManager.getContext())
				.andThrow(
						new DatabaseNotFoundException(
								"This exception is thrown by fake transaction manager."));
		PowerMock.replayAll();
		final Field transactionManagerField = index.getClass()
				.getDeclaredField(TRANSACTION_MANAGER_FIELD_NAME);
		transactionManagerField.setAccessible(true);
		transactionManagerField.set(index, fakeTransactionManager);

		try
		{
			index.count(1);
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertTrue(occurred.getMessage().contains(expected.getMessage()));
		}
		finally
		{
			index.close();
		}
	}
}
