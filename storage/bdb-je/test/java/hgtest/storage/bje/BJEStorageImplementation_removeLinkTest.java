package hgtest.storage.bje;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

/**
 */
public class BJEStorageImplementation_removeLinkTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void removeLinkUsingNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.removeLink(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertEquals(ex.getMessage(),
					"HGStore.remove called with a null handle.");
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void removeLinkWhichIsStored() throws Exception
	{
		startup(3);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.store(first, new HGPersistentHandle[] { second });
		storage.removeLink(first);
		assertFalse(storage.containsLink(first));
		shutdown();
	}

	@Test
	public void removeLinkWhichIsNotStored() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.removeLink(handle);
		assertFalse(storage.containsLink(handle));
		shutdown();
	}

	@Test
	public void throwExceptionWhileRemovingLink() throws Exception
	{
		startup(new IllegalStateException("Throw exception in test case."));
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		try
		{
			storage.removeLink(handle);

		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			final String expectedMessage = String
					.format("Failed to remove value with handle %s: java.lang.IllegalStateException: Throw exception in test case.",
							handle.toString());
			assertEquals(ex.getMessage(), expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
