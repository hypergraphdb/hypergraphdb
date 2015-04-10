package hgtest.storage.HGSortIndex;

import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGIndex;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static hgtest.TestUtils.like2DArray;
import static hgtest.TestUtils.list;
import static hgtest.TestUtils.listAndClose;
import static org.testng.Assert.assertEquals;

/**
 *
 */
public class HGSortIndex_findTest
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
		final List<String> expected = list();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);

		final List<String> actual = listAndClose(index.find(1));

		assertEquals(actual, expected);
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

		final List<String> actual = listAndClose(index.find(65));

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesButDesiredEntryDoesNotExist(
			final Class configuration) throws Exception
	{
		final List<String> expected = list();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(65, "A");
		index.addEntry(66, "B");

		final List<String> actual = listAndClose(index.find(67));

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAndDesiredEntryExists(
			final Class configuration) throws Exception
	{
		final List<String> expected = list("C");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(65, "A");
		index.addEntry(66, "B");
		index.addEntry(67, "C");

		final List<String> actual = listAndClose(index.find(67));

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreEntriesWithTheSameKey(final Class configuration)
			throws Exception
	{
		final List<String> expected = list("ASCII 'B' letter", "B");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(65, "A");
		index.addEntry(66, "B");
		index.addEntry(67, "C");
		index.addEntry(66, "ASCII 'B' letter");

		final List<String> actual = listAndClose(index.find(66));

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}
}