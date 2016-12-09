package hgtest.storage.bje.BJEStorageImplementation;

import static java.lang.String.format;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.junit.After;
import org.junit.Test;

public class BJEStorageImplementation_getIncidenceResultSetTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void throwsException_whenNullHandleIsUsed() throws Exception
	{
		startup();

		below.expect(NullPointerException.class);
		below
				.expectMessage("HGStore.getIncidenceSet called with a null handle.");
		storage.getIncidenceResultSet(null);
	}

	@Test
	public void wrapsUnderlyingException_withHypergraphException()
			throws Exception
	{
		startup(new IllegalStateException("Exception in test case."));

		final HGPersistentHandle handle = new UUIDPersistentHandle();

		below.expect(HGException.class);
		below
				.expectMessage(format(
						"Failed to retrieve incidence set for handle %s: java.lang.IllegalStateException: Exception in test case.",
						handle));
		storage.getIncidenceResultSet(handle);
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
