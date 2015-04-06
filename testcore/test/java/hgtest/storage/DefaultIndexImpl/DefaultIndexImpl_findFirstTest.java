package hgtest.storage.DefaultIndexImpl;

import com.google.code.multitester.annonations.ImportedTest;
import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGIndex;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static hgtest.TestUtils.like2DArray;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 *
 */
public class DefaultIndexImpl_findFirstTest
{

	@DataProvider(name = "configurations")
	public Object[][] provide() throws Exception
	{
		return like2DArray(BJE_DefaultIndexImpl_findFirstConfiguration.class);
	}

	@Test(dataProvider = "configurations")
	public void thereAreNotAddedEntries(final Class configuration)
			throws Exception
	{
		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex<Integer, String> index = tester.importField("underTest",
				HGIndex.class);

		final String found = index.findFirst(28);

		assertNull(found);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneEntry(final Class configuration) throws Exception
	{
		final String expected = "first";

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex<Integer, String> index = tester.importField("underTest",
				HGIndex.class);
		index.addEntry(1, "first");

		final String actual = index.findFirst(1);

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAddedButDesiredEntryDoesNotExist(
			final Class configuration) throws Exception
	{
		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex<Integer, String> index = tester.importField("underTest",
				HGIndex.class);
		index.addEntry(1, "first");
		index.addEntry(2, "second");

		final String found = index.findFirst(50);

		assertNull(found);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAddedAndDesiredEntryExists(
			final Class configuration) throws Exception
	{
		final String expected = "fifty";

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex<Integer, String> index = tester.importField("underTest",
				HGIndex.class);
		index.addEntry(1, "first");
		index.addEntry(2, "second");
		index.addEntry(50, "fifty");

		final String actual = index.findFirst(50);

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreEntriesWithTheSameKey(final Class configuration)
			throws Exception
	{
		final String expected = "second";

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex<Integer, String> index = tester.importField("underTest",
				HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(2, "second");

		final String actual = index.findFirst(2);

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}
}

@ImportedTest(testClass = hgtest.storage.bje.DefaultIndexImpl.DefaultIndexImpl_findFirstTest.class, startupSequence = {
		"up1", "up2", "up3" }, shutdownSequence = { "down1" })
class BJE_DefaultIndexImpl_findFirstConfiguration
{
}
