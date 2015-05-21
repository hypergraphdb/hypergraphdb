package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_removeLinkTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_3")
	public void removeLinkWhichIsStored(final Class configuration)
			throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.store(first, new HGPersistentHandle[] { second });

		storage.removeLink(first);
		assertFalse(storage.containsLink(first));
	}

	@Test(dataProvider = "configurations_2")
	public void removeLinkWhichIsNotStored(final Class configuration)
			throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		storage.removeLink(handle);
		assertFalse(storage.containsLink(handle));
	}
}
