package hgtest.storage.DefaultIndexImpl;

import com.google.code.multitester.annonations.ImportedTest;
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
public class DefaultIndexImpl_scanKeysTest
{
	@DataProvider(name = "configurations")
	public Object[][] provide() throws Exception
	{
		return like2DArray(BJE_DefaultIndexImpl_scanKeysConfiguration.class);
	}

	@Test(dataProvider = "configurations")
	public void thereAreNotAddedEntries(final Class configuration)
			throws Exception
	{
		final List<Integer> expected = list();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);

		final List<Integer> actual = listAndClose(index.scanKeys());

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneAddedEntry(final Class configuration)
			throws Exception
	{
		final List<Integer> expected = list(11);

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(11, "eleven");

		final List<Integer> actual = listAndClose(index.scanKeys());

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralAddedEntries(final Class configuration)
			throws Exception
	{
		final List<Integer> expected = list(1, 2, 3);

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		final List<Integer> actual = listAndClose(index.scanKeys());

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}
}

@ImportedTest(testClass = hgtest.storage.bje.DefaultIndexImpl.DefaultIndexImpl_scanKeysTest.class, startupSequence = {
		"up1", "up2", "up3" }, shutdownSequence = { "down1" })
class BJE_DefaultIndexImpl_scanKeysConfiguration
{
}