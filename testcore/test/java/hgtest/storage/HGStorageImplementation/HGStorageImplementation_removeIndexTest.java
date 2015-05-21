package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.HGIndex;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_removeIndexTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_1")
	public void removeIndexWhichExists(final Class configuration)
			throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final String indexName = "sample index";
		storage.getIndex(indexName, null, null, null, true, true);

		storage.removeIndex(indexName);

		final HGIndex<Object, Object> removedIndex = storage.getIndex(
				indexName, null, null, null, true, false);
		assertNull(removedIndex);
	}
}
