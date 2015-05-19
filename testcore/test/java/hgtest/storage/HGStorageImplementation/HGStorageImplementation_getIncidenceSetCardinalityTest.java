package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author YuriySechko
 */
public class HGStorageImplementation_getIncidenceSetCardinalityTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_1")
	public void thereAreNotIncidenceLinks(final Class configuration)
			throws Exception
	{
        initSpecificStorageImplementation(configuration);

		final long cardinality = storage
				.getIncidenceSetCardinality(new UUIDPersistentHandle());
		assertEquals(cardinality, 0);
	}

	@Test(dataProvider = "configurations_2")
    public void thereIsOneIncidenceLink(final Class configuration)
			throws Exception
	{
        initSpecificStorageImplementation(configuration);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		final long cardinality = storage.getIncidenceSetCardinality(handle);
		assertEquals(cardinality, 1);
	}

	@Test(dataProvider = "configurations_4")
	public void thereAreSeveralIncidenceLinks(final Class configuration)
			throws Exception
	{
        initSpecificStorageImplementation(configuration);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		final long cardinality = storage.getIncidenceSetCardinality(handle);
		assertEquals(cardinality, 3);
	}
}