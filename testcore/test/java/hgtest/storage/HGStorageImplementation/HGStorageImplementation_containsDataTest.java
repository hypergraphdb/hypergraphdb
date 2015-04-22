package hgtest.storage.HGStorageImplementation;

import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static hgtest.TestUtils.like2DArray;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
@PrepareForTest(HGConfiguration.class)
public class HGStorageImplementation_containsDataTest extends PowerMockTestCase
{
	@DataProvider(name = "configurations_1")
	public Object[][] provide1() throws Exception
	{
		return like2DArray(BJE_HGStorageImplementation_1.class,
				BDB_HGStorageImplementation_1.class);
	}

	@DataProvider(name = "configurations_2")
	public Object[][] provide2() throws Exception
	{
		return like2DArray(BJE_HGStorageImplementation_2.class,
				BDB_HGStorageImplementation_2.class);
	}

	@Test(dataProvider = "configurations_2")
	public void arrayOfSeveralItems(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);
		storage.store(handle, new byte[] { 4, 5, 6 });

		assertTrue(storage.containsData(handle));
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void emptyArray(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);
		storage.store(handle, new byte[] {});

		assertTrue(storage.containsData(handle));
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void arrayOfOneItem(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);
		storage.store(handle, new byte[] { 1 });

		assertTrue(storage.containsData(handle));
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_1")
	public void dataIsNotStored(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);

		assertFalse(storage.containsData(handle));
		tester.shutdown();
	}
}
