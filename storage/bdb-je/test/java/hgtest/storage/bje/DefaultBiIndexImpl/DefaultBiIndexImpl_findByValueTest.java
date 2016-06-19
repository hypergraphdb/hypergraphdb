package hgtest.storage.bje.DefaultBiIndexImpl;

import static hgtest.storage.bje.TestUtils.list;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.junit.Test;

public class DefaultBiIndexImpl_findByValueTest extends
		DefaultBiIndexImplTestBasis
{
	@Test
	public void throwsException_whenValueIsNull() throws Exception
	{
		startupIndex();

		try
		{
			below.expect(NullPointerException.class);
			indexImpl.findByValue(null);
		}
		finally
		{
			indexImpl.close();
		}
	}

	@Test
	public void happyPath_thereIsOneEntry() throws Exception
	{
		final List<Integer> expected = asList(1);

		startupIndex();
		indexImpl.addEntry(1, "one");

		final HGRandomAccessResult<Integer> result = indexImpl
				.findByValue("one");

		final List<Integer> actual = list(result);
		assertEquals(expected, actual);

		result.close();
		indexImpl.close();
	}

	@Test
	public void returnsEmptyList_whenThereAreNotAddedEntries() throws Exception
	{
		startupIndex();

		final HGRandomAccessResult<Integer> result = indexImpl
				.findByValue("this value doesn't exist");

		final List<Integer> actualList = list(result);
		assertThat(actualList, is(Collections.<Integer> emptyList()));

		result.close();
		indexImpl.close();
	}

	@Test
	public void returnsLastAddedValue_whenThereAreDuplicatedValues()
			throws Exception
	{
		final List<Integer> expected = asList(2, 3);

		startupIndex();
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
	public void returnsEmptyList_whenThereAreSeveralEntriesButDesiredValueDoesNotExist()
			throws Exception
	{
		startupIndex();
		indexImpl.addEntry(2, "two");
		indexImpl.addEntry(3, "three");

		final HGRandomAccessResult<Integer> result = indexImpl
				.findByValue("none");

		final List<Integer> actualList = list(result);
		assertThat(actualList, is(Collections.<Integer> emptyList()));

		result.close();
		indexImpl.close();
	}

	@Test
	public void returnsLastAddedValue_whenThereSeveralValues_andSomeOfThemAreDuplicated()
			throws Exception
	{
		final List<Integer> expected = asList(2);

		startupIndex();
		indexImpl.addEntry(0, "red");
		indexImpl.addEntry(1, "orange");
		indexImpl.addEntry(2, "yellow");
		indexImpl.addEntry(11, "orange");

		final HGRandomAccessResult<Integer> result = indexImpl
				.findByValue("yellow");

		final List<Integer> actual = list(result);
		assertEquals(expected, actual);

		result.close();
		indexImpl.close();
	}

	@Test
	public void throwsException_whenIndexIsNotOpenedAhead() throws Exception
	{
		replay(mockedStorage);
		indexImpl = new DefaultBiIndexImpl<>(INDEX_NAME, mockedStorage,
				transactionManager, keyConverter, valueConverter, comparator,
				null);

		try
		{
			below.expect(HGException.class);
			below.expectMessage("Attempting to lookup index 'sample_index' while it is closed.");
			indexImpl.findByValue("some value");
		}
		finally
		{
			indexImpl.close();
		}
	}

	@Test
	public void wrapsUnderlyingException_withHypergraphException()
			throws Exception
	{
		startupIndexWithFakeTransactionManager();

		try
		{
			below.expect(HGException.class);
			below.expectMessage("Failed to lookup index 'sample_index': java.lang.IllegalStateException: Transaction manager is fake.");
			indexImpl.findByValue("yellow");
		}
		finally
		{
			indexImpl.close();
		}
	}
}
