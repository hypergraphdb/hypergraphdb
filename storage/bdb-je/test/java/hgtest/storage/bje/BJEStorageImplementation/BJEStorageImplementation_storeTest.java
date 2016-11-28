package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.junit.After;
import org.junit.Test;

import org.hypergraphdb.HGException;

public class BJEStorageImplementation_storeTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void throwsException_whenDataStoredUsingNullHandle()
			throws Exception
	{
		startup(1);

		below.expect(HGException.class);
		below.expectMessage("Failed to store hypergraph raw byte []: java.lang.NullPointerException");
		storage.store(null, new byte[] {});
	}

	@Test
	public void throwsException_whenLinksStoredUsingNullHandle()
			throws Exception
	{
		startup();

		below.expect(NullPointerException.class);
		storage.store(null, new HGPersistentHandle[] {});
	}

	@Test
	public void throwsException_whenDataIsNull() throws Exception
	{
		startup(1);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final byte[] nullData = null;

		below.expect(HGException.class);
		below.expectMessage("Failed to store hypergraph raw byte []: java.lang.IllegalArgumentException: Data field for DatabaseEntry data cannot be null");
		storage.store(handle, nullData);
	}

	@Test
	public void throwsException_whenArrayOfLinksIsNull() throws Exception
	{
		startup();

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = null;

		below.expect(NullPointerException.class);
		storage.store(handle, links);
	}

	@Test
	public void throwsException_whenArrayOfLinksContainsNullHandles()
			throws Exception
	{
		startup();

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { null };

		below.expect(NullPointerException.class);
		storage.store(handle, links);
	}

	@Test
	public void wrapsUnderlyingException_withHypergraphException()
			throws Exception
	{
		startup(new IllegalStateException("Throw exception in test case."));

        final HGPersistentHandle handle = new UUIDPersistentHandle();

        below.expect(HGException.class);
		below.expectMessage("Failed to store hypergraph link: java.lang.IllegalStateException: Throw exception in test case.");
		storage.store(handle, new HGPersistentHandle[] {});
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
