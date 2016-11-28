package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BJEStorageImplementation_removeIncidenceSetTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void throwsException_whenHandleIsNull() throws Exception
	{
		below.expect(HGException.class);
		below
				.expectMessage("Failed to remove incidence set of handle null: java.lang.NullPointerException");
		storage.removeIncidenceSet(null);
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
