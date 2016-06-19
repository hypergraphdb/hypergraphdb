package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;

public class BJEStorageImplementation_addIncidenceLinkTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void throwsException_whenLinkIsNull() throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle link = null;

		below.expect(HGException.class);
		below.expectMessage(format(
				"Failed to update incidence set for handle %s: java.lang.NullPointerException",
				handle));
		storage.addIncidenceLink(handle, link);
	}

	@Before
	public void startup() throws Exception
	{
		super.startup();
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
