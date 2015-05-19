package hgtest.storage.HGStorageImplementation;

import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_containsLinkTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_2")
	public void checkExistenceOfStoredLinkFromFirstToSecond(
			final Class configuration) throws Exception
	{
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };
		initSpecificStorageImplementation(configuration);

		storage.store(first, links);

		assertTrue(storage.containsLink(first));
	}

	@Test(dataProvider = "configurations_2")
	public void checkExistenceOfStoredLinkFromSecondToFirst(
			final Class configuration) throws Exception
	{

		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };

		initSpecificStorageImplementation(configuration);
		storage.store(first, links);

		assertFalse(storage.containsLink(second));
	}

	@Test(dataProvider = "configurations_2")
	public void checkExistenceOfHandleWhichIsLinkedToItself(
			final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final MultiTester tester = new MultiTester(configuration);
        initSpecificStorageImplementation(configuration);

		storage.store(handle, new HGPersistentHandle[] { handle });

		assertTrue(storage.containsLink(handle));
	}

	@Test(dataProvider = "configurations_1")
	public void checkExistenceOfNonStoredLink(final Class configuration)
			throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();
        initSpecificStorageImplementation(configuration);

		assertFalse(storage.containsLink(handle));
	}

	@Test(dataProvider = "configurations_2")
	public void arrayOfStoredLinksIsEmpty(final Class configuration)
			throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();
        initSpecificStorageImplementation(configuration);

		storage.store(handle, new HGPersistentHandle[] {});

		assertTrue(storage.containsLink(handle));
	}
}
