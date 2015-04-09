package hgtest.storage.DefaultIndexImpl;

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
public class DefaultIndexImpl_removeAllEntriesTest
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

		index.removeAllEntries(0);

		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneAddedEntry(final Class configuration)
			throws Exception
	{
		final List<String> expected = list();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");

		index.removeAllEntries(1);

		final List<String> actual = listAndClose(index.find(1));
		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesButDesiredEntryDoesNotExist(
			final Class configuration) throws Exception
	{
		final List<String> expected = list("one", "two");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(2, "two");

		index.removeAllEntries(5);

		// read all entries for each key, collect them into one list and compare
		// with expected list
		final List<String> actual = list();
		actual.addAll(listAndClose(index.find(1)));
		actual.addAll(listAndClose(index.find(2)));
		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAndDesiredEntryExists(
			final Class configuration) throws Exception
	{
		final List<String> expected = list("one", "three");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		index.removeAllEntries(2);

		final List<String> actual = list();
		actual.addAll(listAndClose(index.find(1)));
		actual.addAll(listAndClose(index.find(2)));
		actual.addAll(listAndClose(index.find(3)));
		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreEntriesWithTheSameKey(final Class configuration)
			throws Exception
	{
		final List<String> expected = list("one", "two");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");
		index.addEntry(3, "third");

		index.removeAllEntries(3);

		final List<String> actual = list();
		actual.addAll(listAndClose(index.find(1)));
		actual.addAll(listAndClose(index.find(2)));
		actual.addAll(listAndClose(index.find(3)));
		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}
}
