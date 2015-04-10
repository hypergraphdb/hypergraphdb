package hgtest.storage.HGSortIndex;

import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGSortIndex;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static hgtest.TestUtils.*;
import static org.testng.Assert.assertEquals;

/**
 *
 */
public class HGSortIndex_findLTETest
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
		final HGSortIndex index = tester.importField("underTest",
				HGSortIndex.class);

		final List<String> actual = listAndClose(index.findLTE(2));

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneEntryAddedButItIsEqualToDesired(
			final Class configuration) throws Exception
	{
		final Exception expected = new HGException(
				"Failed to lookup index 'sample_index': java.lang.NullPointerException");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGSortIndex index = tester.importField("underTest",
				HGSortIndex.class);
		index.addEntry(2, "A");

		try
		{
			index.findLTE(2);

		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			index.close();
			tester.shutdown();
		}
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneEntryAddedButItIsLessThanDesired(
			final Class configuration) throws Exception
	{
		final List<String> expected = list("A");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGSortIndex index = tester.importField("underTest",
				HGSortIndex.class);
		index.addEntry(2, "A");

		final List<String> actual = listAndClose(index.findLTE(3));

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereIsOneEntryAddedButItIsGreaterThanDesired(
			final Class configuration) throws Exception
	{
		final Exception expected = new HGException(
				"Failed to lookup index 'sample_index': java.lang.NullPointerException");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGSortIndex index = tester.importField("underTest",
				HGSortIndex.class);
		index.addEntry(4, "A");

		try
		{
			index.findLTE(3);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			index.close();
			tester.shutdown();
		}
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAddedButAllOfThemAreLessThanDesired(
			final Class configuration) throws Exception
	{
		final List<String> expected = list("B", "A");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGSortIndex index = tester.importField("underTest",
				HGSortIndex.class);
		index.addEntry(3, "B");
		index.addEntry(2, "A");

		final List<String> actual = listAndClose(index.findLTE(4));

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAddedButAllOfThemAreGreaterThanDesired(
			final Class configuration) throws Exception
	{
		final Exception expected = new HGException(
				"Failed to lookup index 'sample_index': java.lang.NullPointerException");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGSortIndex index = tester.importField("underTest",
				HGSortIndex.class);
		index.addEntry(2, "A");
		index.addEntry(3, "B");

		try
		{
			index.findLTE(1);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			index.close();
			tester.shutdown();
		}
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAddedButAllOfThemAreEqualToDesired(
			final Class configuration) throws Exception
	{
		final Exception expected = new HGException(
				"Failed to lookup index 'sample_index': java.lang.NullPointerException");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGSortIndex index = tester.importField("underTest",
				HGSortIndex.class);
		index.addEntry(3, "B");
		index.addEntry(3, "A");

		try
		{
			index.findLTE(3);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			index.close();
			tester.shutdown();
		}
	}

	@Test(dataProvider = "configurations")
	public void thereAreSeveralEntriesAdded(final Class configuration)
			throws Exception
	{
		final List<String> expected = list("D", "C", "B", "A");

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGSortIndex index = tester.importField("underTest",
				HGSortIndex.class);
		index.addEntry(4, "D");
		index.addEntry(2, "B");
		index.addEntry(1, "A");
		index.addEntry(3, "C");

		final List<String> actual = listAndClose(index.findLTE(5));

		assertEquals(actual, expected);
		index.close();
		tester.shutdown();
	}
}