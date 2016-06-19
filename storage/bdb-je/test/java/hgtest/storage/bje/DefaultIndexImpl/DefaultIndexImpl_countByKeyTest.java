package hgtest.storage.bje.DefaultIndexImpl;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Field;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.junit.Test;

import com.sleepycat.je.DatabaseNotFoundException;

public class DefaultIndexImpl_countByKeyTest extends DefaultIndexImplTestBasis
{
	@Test
	public void throwsException_whenIndexIsNotOpenedAhead() throws Exception
	{
		replay(mockedStorage);
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<>(
				INDEX_NAME, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		below.expect(NullPointerException.class);
		index.count(1);
	}

	@Test
	public void throwsException_whenKeyIsNull() throws Exception
	{
		startupIndex();

		try
		{
			below.expect(NullPointerException.class);
			index.count(null);
		}
		finally
		{
			index.close();
		}
	}

	@Test
	public void returnsZero_whenThereAreNotAddedEntries() throws Exception
	{
		startupIndex();

		final long actualCount = index.count(1);
		assertThat(actualCount, is(0L));

		index.close();
	}

	@Test
	public void returnsZero_whenThereIsOneEntryAdded_butItIsNotEqualToDesired()
			throws Exception
	{
		startupIndex();
		index.addEntry(1, "one");

		final long actualCount = index.count(2);
		assertThat(actualCount, is(0L));

		index.close();
	}

	@Test
	public void happyPath_thereIsOneEntryAdded_andItIsEqualToDesired()
			throws Exception
	{
		final long expected = 1;

		startupIndex();
		index.addEntry(1, "one");

		final long actual = index.count(1);

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void returnsZero_whenThereAreSeveralEntriesAdded_butThereAreNotDesired()
			throws Exception
	{
		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		final long actualCount = index.count(5);
		assertThat(actualCount, is(0L));

		index.close();
	}

	@Test
	public void countsUniqueKeys_whenThereAreSeveralEntriesAdded_andThereAreNotDuplicatedKeys()
			throws Exception
	{
		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		final long actualCount = index.count(3);
		assertThat(actualCount, is(1L));

		index.close();
	}

	@Test
	public void countUniqueKeys_whenThereAreSeveralEntriesAdded_andSomeKeysAreDuplicated()
			throws Exception
	{
		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");
		index.addEntry(3, "third");

		final long actualCount = index.count(3);
		assertThat(actualCount, is(2L));

		index.close();
	}

	@Test
	public void wrapsUnderlyingException_whenHypergraphException()
			throws Exception
	{
		// create index and open it in usual way
		startupIndex();
		verify(mockedStorage);
		reset(mockedStorage);

		// after index is opened inject fake transaction manager
		final HGTransactionManager fakeTransactionManager = createStrictMock(HGTransactionManager.class);
		expect(fakeTransactionManager.getContext())
				.andThrow(
						new DatabaseNotFoundException(
								"This exception is thrown by fake transaction manager."));
		replay(mockedStorage, fakeTransactionManager);
		final Field transactionManagerField = index.getClass()
				.getDeclaredField(FieldNames.TRANSACTION_MANAGER);
		transactionManagerField.setAccessible(true);
		transactionManagerField.set(index, fakeTransactionManager);

		try
		{
			below.expect(HGException.class);
			below.expectMessage("This exception is thrown by fake transaction manager.");
			index.count(1);
		}
		finally
		{
			index.close();
		}
	}
}
