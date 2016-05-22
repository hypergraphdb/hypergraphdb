package hgtest.storage.bje.BJEStorageImplementation;

import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.StringContains.containsString;

import org.hypergraphdb.HGException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BJEStorageImplementation_removeIndexTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void throwsException_whenIndexNameIsNull() throws Exception
	{
		below.expect(HGException.class);
		below
				.expectMessage(allOf(
						containsString("com.sleepycat.je.DatabaseNotFoundException"),
						containsString("Attempted to remove non-existent database hgstore_idx_null")));

		storage.removeIndex(null);
	}

	@Test
	public void throwsException_whenIndexWhichIsNotStoredAhead()
			throws Exception
	{
		below.expect(HGException.class);
		below
				.expectMessage(allOf(
						containsString("com.sleepycat.je.DatabaseNotFoundException"),
						containsString("Attempted to remove non-existent database hgstore_idx_This index does not exist")));

		storage.removeIndex("This index does not exist");
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
