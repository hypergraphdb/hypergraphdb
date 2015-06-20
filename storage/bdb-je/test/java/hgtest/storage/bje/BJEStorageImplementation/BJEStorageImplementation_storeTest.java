package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_storeTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void storeDataUsingNullHandle() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to store hypergraph raw byte []: java.lang.NullPointerException");

		startup(1);
		try
		{
			storage.store(null, new byte[] {});
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void storeLinksUsingNullHandle() throws Exception
	{
		final Exception expected = new NullPointerException();

		startup();
		try
		{
			storage.store(null, new HGPersistentHandle[] {});
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void dataIsNull() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to store hypergraph raw byte []: java.lang.IllegalArgumentException: Data field for DatabaseEntry data cannot be null");

		startup(1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final byte[] nullData = null;
		try
		{
			storage.store(handle, nullData);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void arrayOfLinksIsNull() throws Exception
	{
        final Exception expected = new NullPointerException();

		startup();
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = null;
		try
		{
			storage.store(handle, links);
		}
		catch (Exception occurred)
		{
            assertExceptions(occurred, expected);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void arrayOfLinksContainsNullHandles() throws Exception
	{
		startup();
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { null };
		try
		{
			storage.store(handle, links);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertNull(ex.getMessage());
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void throwExceptionWhileStoringLinks() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to store hypergraph link: java.lang.IllegalStateException: Throw exception in test case.");

		startup(new IllegalStateException("Throw exception in test case."));
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		try
		{
			storage.store(handle, new HGPersistentHandle[] {});
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			shutdown();
		}
	}
}
