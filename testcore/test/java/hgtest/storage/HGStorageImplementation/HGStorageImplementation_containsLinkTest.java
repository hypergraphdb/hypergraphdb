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

import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;

import static hgtest.TestUtils.like2DArray;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
@PrepareForTest(HGConfiguration.class)
public class HGStorageImplementation_containsLinkTest extends PowerMockTestCase
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
	public void checkExistenceOfStoredLinkFromFirstToSecond(
			final Class configuration) throws Exception
	{
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);
		storage.store(first, links);

		assertTrue(storage.containsLink(first));
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void checkExistenceOfStoredLinkFromSecondToFirst(
			final Class configuration) throws Exception
	{

		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);
		storage.store(first, links);

		assertFalse(storage.containsLink(second));
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void checkExistenceOfHandleWhichIsLinkedToItself(
			final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);
		storage.store(handle, new HGPersistentHandle[] { handle });

		assertTrue(storage.containsLink(handle));
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_1")
	public void checkExistenceOfNonStoredLink(final Class configuration)
			throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);

		assertFalse(storage.containsLink(handle));
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void arrayOfStoredLinksIsEmpty(final Class configuration)
			throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);
		storage.store(handle, new HGPersistentHandle[] {});

		assertTrue(storage.containsLink(handle));
		tester.shutdown();
	}
}
