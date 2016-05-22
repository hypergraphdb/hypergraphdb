package hgtest.storage.bje.BJEStorageImplementation;

import static java.lang.String.format;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BJEStorageImplementation_removeIncidenceLinkTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void throwsException_whenHandleIsNull() throws Exception
	{
		final HGPersistentHandle handle = null;
		final HGPersistentHandle link = new UUIDPersistentHandle();

		below.expect(HGException.class);
		below
				.expectMessage("Failed to update incidence set for handle null: java.lang.NullPointerException");
		storage.removeIncidenceLink(handle, link);
	}

	@Test
	public void throwsException_whenBothLinkAndHandleAreNull() throws Exception
	{
		below.expect(HGException.class);
		below
				.expectMessage("Failed to update incidence set for handle null: java.lang.NullPointerException");
		storage.removeIncidenceLink(null, null);
	}

	@Test
	public void throwsException_whenLinkIsNull() throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle link = null;

		below.expect(HGException.class);
		below
				.expectMessage(format(
						"Failed to update incidence set for handle %s: java.lang.NullPointerException",
						handle));
		storage.removeIncidenceLink(handle, link);
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
