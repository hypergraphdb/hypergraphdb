package hgtest.storage.DefaultIndexImpl;

import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGIndex;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static hgtest.TestUtils.like2DArray;
import static org.testng.Assert.assertEquals;

/**
 *
 */
public class DefaultIndexImpl_countByKeyTest
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

		final long actual = index.count(1);

		assertEquals(actual, expected);
		index.close();
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneEntryAddedButItIsNotEqualToDesired(
			final Class configuration) throws Exception
	{
		final long expected = 0;

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");

		final long actual = index.count(2);

		assertEquals(actual, expected);
		index.close();
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneEntryAddedAndItIsEqualToDesired(
			final Class configuration) throws Exception
	{
		final long expected = 1;

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);

		index.addEntry(1, "one");

		final long actual = index.count(1);

		assertEquals(actual, expected);
		index.close();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAddedButThereAreNotDesired(
			final Class configuration) throws Exception
	{
		final long expected = 0;

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		final long actual = index.count(5);

		assertEquals(actual, expected);
		index.close();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAddedButThereAreNotDuplicatedKeys(
			final Class configuration) throws Exception
	{
		final long expected = 1;

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		final long actual = index.count(3);

		assertEquals(actual, expected);
		index.close();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAddedAndSomeKeysAreDuplicated(
			final Class configuration) throws Exception
	{
		final long expected = 2;

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGIndex index = tester.importField("underTest", HGIndex.class);
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");
		index.addEntry(3, "third");

		final long actual = index.count(3);

		assertEquals(actual, expected);
		index.close();
	}
}