package hgtest.storage.HGStorageImplementation;

import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static hgtest.TestUtils.like2DArray;
import static org.testng.Assert.assertEquals;

/**
 * @author YuriySechko
 */
@PrepareForTest(HGConfiguration.class)
public class HGStorageImplementation_getIncidenceSetCardinalityTest extends
        PowerMockTestCase
{
    @DataProvider(name = "configurations_1")
    public Object[][] provide1() throws Exception
    {
        return like2DArray(BJE_HGStorageImplementation_1.class,
                BDB_HGStorageImplementation_1.class);
    }

    @DataProvider(name = "configurations_2")
    public Object[][] provide2() throws Exception
    {
        return like2DArray(BJE_HGStorageImplementation_2.class,
                BDB_HGStorageImplementation_2.class);
    }

    @DataProvider(name = "configurations_4")
    public Object[][] provide4() throws Exception
    {
        return like2DArray(BJE_HGStorageImplementation_4.class,
                BDB_HGStorageImplementation_4.class);
    }

    @Test(dataProvider = "configurations_1")
    public void thereAreNotIncidenceLinks(final Class configuration) throws Exception
    {
        final MultiTester tester = new MultiTester(configuration);
        tester.startup();
        final HGStoreImplementation storage = tester.importField("underTest", HGStoreImplementation.class);

        final long cardinality = storage
                .getIncidenceSetCardinality(new UUIDPersistentHandle());
        assertEquals(cardinality, 0);
           tester.shutdown();
    }

    @Test(dataProvider = "configurations_2")
    public void thereIsOneIncidenceLink(final Class configuration) throws Exception
    {
        final MultiTester tester = new MultiTester(configuration);
        tester.startup();
        final HGStoreImplementation storage = tester.importField("underTest", HGStoreImplementation.class);

        final HGPersistentHandle handle = new UUIDPersistentHandle();
        storage.addIncidenceLink(handle, new UUIDPersistentHandle());
        final long cardinality = storage.getIncidenceSetCardinality(handle);
        assertEquals(cardinality, 1);
        tester.shutdown();
    }

    @Test(dataProvider = "configurations_4")
    public void thereAreSeveralIncidenceLinks(final Class configuration) throws Exception
    {
        final MultiTester tester = new MultiTester(configuration);
        tester.startup();
        final HGStoreImplementation storage = tester.importField("underTest", HGStoreImplementation.class);

        final HGPersistentHandle handle = new UUIDPersistentHandle();
        storage.addIncidenceLink(handle, new UUIDPersistentHandle());
        storage.addIncidenceLink(handle, new UUIDPersistentHandle());
        storage.addIncidenceLink(handle, new UUIDPersistentHandle());
        final long cardinality = storage.getIncidenceSetCardinality(handle);
        assertEquals(cardinality, 3);
        tester.shutdown();
    }
}