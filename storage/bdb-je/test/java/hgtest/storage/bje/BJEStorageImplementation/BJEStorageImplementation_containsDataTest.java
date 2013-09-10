package hgtest.storage.bje.BJEStorageImplementation;

import com.sleepycat.je.DatabaseNotFoundException;
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
public class BJEStorageImplementation_containsDataTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void useNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.containsData(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertEquals(ex.getMessage(), null);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void arrayOfSeveralItems() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 4, 5, 6 });
		assertTrue(storage.containsData(handle));
		shutdown();
	}

	@Test
	public void emptyArray() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		assertTrue(storage.containsData(handle));
		shutdown();
	}

	@Test
	public void arrayOfOneItem() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 1 });
		assertTrue(storage.containsData(handle));
		shutdown();
	}

	@Test
	public void dataIsNotStored() throws Exception
	{
		startup(1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		assertFalse(storage.containsData(handle));
		shutdown();
	}

	@Test
	public void exceptionIsThrown() throws Exception
	{
		startup(new DatabaseNotFoundException("Exception in test case."));
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		try
		{
			storage.containsData(handle);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			final String expectedMessage = String
					.format("Failed to retrieve link with handle %s: com.sleepycat.je.DatabaseNotFoundException: (JE 5.0.34) Exception in test case.",
							handle);
			assertEquals(ex.getMessage(), expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
