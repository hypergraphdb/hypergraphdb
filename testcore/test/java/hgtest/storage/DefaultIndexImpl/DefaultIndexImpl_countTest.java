package hgtest.storage.DefaultIndexImpl;

import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGIndex;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_countTest
{
	@DataProvider(name = "configurations")
	public Object[][] provide() throws Exception
	{
		return new Object[][] {
				{ BJE_DefaultIndexImpl_countTestConfiguration.class },
				{ BDB_DefaultIndexImpl_countTestConfiguration.class } };
	}

	@Test(dataProvider = "configurations")
	public void printLibraryPath(final Class configuration)
	{
        System.out.println(">>>>>>>>>>>>>> java.library.path=" + System.getProperty("java.library.path"));
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
}
