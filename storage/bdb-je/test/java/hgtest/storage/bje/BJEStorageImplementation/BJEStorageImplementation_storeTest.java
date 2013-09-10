package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 */
public class BJEStorageImplementation_storeTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void storeArrayOfBytes() throws Exception
	{
		final byte[] expected = new byte[] { 4, 5, 6 };
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 4, 5, 6 });
		final byte[] stored = storage.getData(handle);
		assertEquals(stored, expected);
		shutdown();
	}

	@Test
	public void storeEmptyArray() throws Exception
	{
		final byte[] expected = new byte[] {};
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		final byte[] stored = storage.getData(handle);
		assertEquals(stored, expected);
		shutdown();
	}

	@Test
	public void storeArrayOfOneByte() throws Exception
	{
		final byte[] expected = new byte[] { 22 };
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 22 });
		final byte[] stored = storage.getData(handle);
		assertEquals(stored, expected);
	}

	@Test
	public void storeDataUsingNullHandle() throws Exception
	{
		startup(1);
		try
		{
			storage.store(null, new byte[] {});
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			assertEquals(ex.getMessage(),
					"Failed to store hypergraph raw byte []: java.lang.NullPointerException");
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void storeLinksUsingNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.store(null, new HGPersistentHandle[] {});
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
	public void dataIsNull() throws Exception
	{
		startup(1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final byte[] nullData = null;
		try
		{
			storage.store(handle, nullData);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			assertEquals(
					ex.getMessage(),
					"Failed to store hypergraph raw byte []: java.lang.IllegalArgumentException: Data field for DatabaseEntry data cannot be null");
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void arrayOfLinksIsNull() throws Exception
	{
		startup();
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = null;
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
	public void arrayOfLinksIsEmpty() throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {};
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new HGPersistentHandle[] {});
		final HGPersistentHandle[] stored = storage.getLink(handle);
		assertEquals(stored, expected);
		shutdown();
	}

	@Test
	public void storeOneLink() throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] { new UUIDPersistentHandle() };
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { expected[0] };
		storage.store(handle, links);
		final HGPersistentHandle[] stored = storage.getLink(handle);
		assertEquals(stored, expected);
		shutdown();
	}

	@Test
	public void storeSeveralLinks() throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle(),
				new UUIDPersistentHandle() };
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				expected[0], expected[1], expected[2] };
		storage.store(handle, links);
		final HGPersistentHandle[] stored = storage.getLink(handle);
		assertEquals(stored, expected);
		shutdown();
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
		startup(new IllegalStateException("Throw exception in test case."));
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		try
		{
			storage.store(handle, new HGPersistentHandle[] {});
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			assertEquals(
					ex.getMessage(),
					"Failed to store hypergraph link: java.lang.IllegalStateException: Throw exception in test case.");
		}
		finally
		{
			shutdown();
		}
	}

}
