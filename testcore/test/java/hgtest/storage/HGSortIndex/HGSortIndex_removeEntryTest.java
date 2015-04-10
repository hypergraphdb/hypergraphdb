package hgtest.storage.HGSortIndex;

import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGIndex;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static hgtest.TestUtils.like2DArray;
import static hgtest.TestUtils.list;
import static hgtest.TestUtils.listAndClose;
import static org.testng.Assert.assertEquals;

/**
 *
 */
public class HGSortIndex_removeEntryTest
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
		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);

		index.removeEntry(22, "twenty two");

		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneDesiredEntry(final Class configuration)
			throws Exception
	{
		final List<String> expected = list();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(2, "two");

		index.removeEntry(2, "two");

		final List<String> actual = listAndClose(index.find(2));
		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesButDesiredEntryDoesNotExist(
			final Class configuration) throws Exception
	{
		final List<String> expected = list("one", "two", "three");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		index.removeEntry(5, "five");

		// collect all existing entries into one list then compare it with
		// expected list
		final List<String> actual = list();
		actual.addAll(listAndClose(index.find(1)));
		actual.addAll(listAndClose(index.find(2)));
		actual.addAll(listAndClose(index.find(3)));
		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAddedAndDesiredEntryExists(
			final Class configuration) throws Exception
	{
		final List<String> expected = list("one", "two");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		index.removeEntry(3, "three");

		final List<String> actual = list();
		actual.addAll(listAndClose(index.find(1)));
		actual.addAll(listAndClose(index.find(2)));
		actual.addAll(listAndClose(index.find(3)));
		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreDuplicatedEntries(final Class configuration)
			throws Exception
	{
		final List<String> expected = list("two");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(1, "one");
		index.addEntry(2, "two");

		index.removeEntry(1, "one");

		final List<String> actual = list();
		actual.addAll(listAndClose(index.find(1)));
		actual.addAll(listAndClose(index.find(2)));
		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}
}
