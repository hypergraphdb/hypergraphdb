package hgtest.storage.HGSortIndex;

import com.google.code.multitester.annonations.ImportedTest;
import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGRandomAccessResult;
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
public class HGSortIndex_scanValuesTest
{
	@DataProvider(name = "configurations")
	public Object[][] provide() throws Exception
	{
		return like2DArray(BJE_DefaultIndexImpl_scanValuesConfiguration.class);
	}

	@Test(dataProvider = "configurations")
	public void thereAreNotAddedEntries(final Class configuration)
			throws Exception
	{
		final List<String> expected = list();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);

		final HGRandomAccessResult<String> result = index.scanValues();
		final List<String> actual = list(result);

		assertEquals(actual, expected);
		result.close();
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneEntryAdded(final Class configuration)
			throws Exception
	{
		final List<String> expected = list("first");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "first");

		final List<String> actual = listAndClose(index.scanValues());

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAdded(final Class configuration)
			throws Exception
	{
		final List<String> expected = list("first", "second", "third");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "first");
		index.addEntry(2, "second");
		index.addEntry(3, "third");

		final List<String> actual = listAndClose(index.scanValues());

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}
}

@ImportedTest(testClass = hgtest.storage.bje.DefaultIndexImpl.DefaultIndexImpl_scanValuesTest.class, startupSequence = {
		"up1", "up2", "up3" }, shutdownSequence = { "down1" })
class BJE_DefaultIndexImpl_scanValuesConfiguration
{
}