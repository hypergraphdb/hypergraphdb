package hgtest.storage.bje.DefaultBiIndexImpl;

import static java.util.Arrays.asList;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.junit.Test;

public class DefaultBiIndexImpl_addEntryTest extends
		DefaultBiIndexImplTestBasis
{
	@Test
	public void throwsException_whenKeyIsNull() throws Exception
	{
		startupIndex();

		try
		{
			below.expect(NullPointerException.class);
			indexImpl.addEntry(null, "some string");
		}
		finally
		{
			indexImpl.close();
		}
	}

	@Test
	public void throwsException_whenValueIsNull() throws Exception
	{
		startupIndex();

		try
		{
			below.expect(NullPointerException.class);
			indexImpl.addEntry(48, null);
		}
		finally
		{
			indexImpl.close();
		}
	}

	@Test
	public void throwsException_whenIndexIsNotOpenedAhead() throws Exception
	{
		replay(mockedStorage);

		final DefaultBiIndexImpl<Integer, String> indexImplSpecificForThisTestCase = new DefaultBiIndexImpl<>(
				INDEX_NAME, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		below.expect(HGException.class);
		below.expectMessage("Attempting to operate on index 'sample_index' while the index is being closed.");
		indexImplSpecificForThisTestCase.addEntry(2, "two");
	}

	@Test
	public void wrapsTransactionManagerException_withHypergraphException()
			throws Exception
	{
		startupIndexWithFakeTransactionManager();

		try
		{
			below.expect(HGException.class);
			below.expectMessage("Failed to add entry to index 'sample_index': java.lang.IllegalStateException: Transaction manager is fake.");
			indexImpl.addEntry(2, "two");
		}
		finally
		{
			indexImpl.close();
		}
	}

	@Test
	public void happyPath_addOneEntry() throws Exception
	{
		startupIndex();

		indexImpl.addEntry(1, "one");

		final String storedData = indexImpl.getData(1);
		assertThat(storedData, is("one"));

		indexImpl.close();
	}

	@Test
	public void happyPath_addSeveralDifferentEntries() throws Exception
	{
		final List<String> expected = asList("twenty two", "thirty three",
				"forty four");

		startupIndex();

		indexImpl.addEntry(22, "twenty two");
		indexImpl.addEntry(33, "thirty three");
		indexImpl.addEntry(44, "forty four");

		final List<String> stored = asList(indexImpl.getData(22),
				indexImpl.getData(33), indexImpl.getData(44));

		assertEquals(expected, stored);

		indexImpl.close();
	}

	@Test
	public void theLastStoredValueOverwritesExisting_whenKeysAreTheSame()
			throws Exception
	{
		startupIndex();

		indexImpl.addEntry(4, "first value");
		indexImpl.addEntry(4, "second value");

		final String storedData = indexImpl.getData(4);

		assertThat(storedData, is("second value"));

		indexImpl.close();
	}
}
