package hgtest.storage.DefaultIndexImpl;

import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGIndex;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static hgtest.TestUtils.like2DArray;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_countTest
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
		final long expected = 0;

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);

		final long actual = index.count();

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneAddedEntry(final Class configuration)
			throws Exception
	{
		final long expected = 1;

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);

		index.addEntry(0, "0");

		final long actual = index.count();

		assertEquals(actual, expected);
		index.close();
	}

	@Test(dataProvider = "configurations")
	public void thereAreTwoEntriesAdded(final Class configuration)
			throws Exception
	{
		final long expected = 2;

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);

		index.addEntry(0, "0");
		index.addEntry(1, "1");

		final long actual = index.count();

		assertEquals(actual, expected);
		index.close();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAdded(final Class configuration)
			throws Exception
	{
		final long expected = 3;

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);

		index.addEntry(0, "0");
		index.addEntry(1, "1");
		index.addEntry(2, "2");

		final long actual = index.count();

		assertEquals(actual, expected);
		index.close();
	}
}
