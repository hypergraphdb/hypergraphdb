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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
@PrepareForTest(HGConfiguration.class)
public class HGStorageImplementation_getDataTest extends PowerMockTestCase
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

	@Test(dataProvider = "configurations_1")
	public void readDataWhichIsNotStored(final Class configuration)
			throws Exception
	{
		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final byte[] retrieved = storage.getData(handle);
		assertNull(retrieved);
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void readEmptyArray(final Class configuration) throws Exception
	{
		final byte[] expected = new byte[] {};

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		final byte[] retrieved = storage.getData(handle);
		assertEquals(retrieved, expected);
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void readArrayWhichContainsOneItem(final Class configuration)
			throws Exception
	{
		final byte[] expected = new byte[] { 44 };

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 44 });
		final byte[] retrieved = storage.getData(handle);
		assertEquals(retrieved, expected);
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void readArrayWhichContainsSeveralItems(final Class configuration)
			throws Exception
	{
		final byte[] expected = new byte[] { 11, 22, 33 };

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 11, 22, 33 });
		final byte[] retrieved = storage.getData(handle);
		assertEquals(retrieved, expected);
		tester.shutdown();
	}
}
