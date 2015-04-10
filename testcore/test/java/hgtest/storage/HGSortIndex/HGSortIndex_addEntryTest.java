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
public class HGSortIndex_addEntryTest
{
	@DataProvider(name = "configurations")
	public Object[][] provide() throws Exception
	{
		return like2DArray(BJE_DefaultIndexImpl_configuration.class);
	}

	@Test(dataProvider = "configurations")
	public void addOneEntry(final Class configuration) throws Exception
	{
		final List<String> expected = list("twenty two");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(22, "twenty two");

		final List<String> actual = listAndClose(index.find(22));
		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void addSeveralDifferentEntries(final Class configuration)
			throws Exception
	{
		final List<String> expected = list("one", "two", "three");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		// read actual stored data entry by entry
		final List<String> actual = list();
		actual.addAll(listAndClose(index.find(1)));
		actual.addAll(listAndClose(index.find(2)));
		actual.addAll(listAndClose(index.find(3)));
		assertEquals(actual, expected);
		index.close();
        tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void addDuplicatedKeys(final Class configuration) throws Exception
	{
		final List<String> expected = list("another one", "one");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);

		index.addEntry(1, "one");
		index.addEntry(1, "another one");
		index.addEntry(2, "two");

		List<String> actual = listAndClose(index.find(1));
		assertEquals(actual, expected);
		index.close();
        tester.shutdown();
	}
}
