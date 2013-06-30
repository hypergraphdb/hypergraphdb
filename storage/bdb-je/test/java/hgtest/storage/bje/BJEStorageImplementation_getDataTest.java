package hgtest.storage.bje;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

/**
 */
public class BJEStorageImplementation_getDataTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void readDataWhichIsNotStoredUsingGivenHandle() throws Exception
	{
		startup(1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final byte[] retrievedData = storage.getData(handle);
		assertNull(retrievedData);
		shutdown();
	}
}
