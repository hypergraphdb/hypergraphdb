package hgtest.storage.DefaultIndexImpl;

import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGRandomAccessResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static hgtest.TestUtils.like2DArray;
import static hgtest.TestUtils.list;
import static org.testng.Assert.assertEquals;

/**
 *
 */
public class DefaultIndexImpl_findTest
{
	@DataProvider(name = "configurations")
	public Object[][] provide() throws Exception
	{
		return like2DArray(BJE_DefaultIndexImpl_configuration.class);
	}

	@Test(dataProvider = "configurations")
	public void thereAreNotAddedEntries(final Class configuration)
			throws Exception
	{
		final List<String> expected = Collections.emptyList();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);

		final HGRandomAccessResult<String> result = index.find(1);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneAddedEntry(final Class configuration)
			throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("A");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(65, "A");

		final HGRandomAccessResult<String> result = index.find(65);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesButDesiredEntryDoesNotExist(
			final Class configuration) throws Exception
	{
		final List<String> expected = Collections.emptyList();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(65, "A");
		index.addEntry(66, "B");

		final HGRandomAccessResult<String> result = index.find(67);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAndDesiredEntryExists(
			final Class configuration) throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("C");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(65, "A");
		index.addEntry(66, "B");
		index.addEntry(67, "C");

		final HGRandomAccessResult<String> result = index.find(67);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreEntriesWithTheSameKey(final Class configuration)
			throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("ASCII 'B' letter");
		expected.add("B");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(65, "A");
		index.addEntry(66, "B");
		index.addEntry(67, "C");
		index.addEntry(66, "ASCII 'B' letter");

		final HGRandomAccessResult<String> result = index.find(66);
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
		tester.shutdown();
	}
}