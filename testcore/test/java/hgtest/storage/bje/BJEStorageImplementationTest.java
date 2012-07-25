package hgtest.storage.bje;

import com.sleepycat.je.DatabaseConfig;
import org.easymock.EasyMock;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import com.sleepycat.je.Environment;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * This class contains unit tests for {@link BJEStorageImplementation}. For this
 * purpose we use PowerMock + EasyMock test framework (see <a
 * href="http://code.google.com/p/powermock/">PowerMock page on Google Code</a>
 * and <a href="http://easymock.org/">EasyMock home page</a> for details).
 * EasyMock's capabilities are sufficient for interfaces and plain classes. But
 * some classes in hypergraphdb modules are final so we cannot mock it in usual
 * way. PowerMock allows to create mocks even for final classes. So we can test
 * {@link BJEStorageImplementation} in isolation from other environment and in
 * most cases (if it is needed) from other classes.
 *
 * @author Yuriy Sechko
 */

@PrepareForTest(HGConfiguration.class)
public class BJEStorageImplementationTest extends PowerMockTestCase
{
    private static final String TEMP_DATABASE_DIRECTORY = "hgtest.tmp";
    private static final String HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME = "org.hypergraphdb.handle.UUIDHandleFactory";

    // location for temporary directory for tests
    private String testDatabaseLocation;

    /**
     * Deletes temporary directory used during tests
     */
    @BeforeMethod
    @AfterMethod
    private void deleteTestDatabaseDirectory()
    {
        testDatabaseLocation = System.getProperty("user.home") + File.separator + "hgtest.tmp";
        File testDatabaseDir = new File(testDatabaseLocation);
        testDatabaseDir.delete();
    }

    // @Test
    // public void testGetConfiguration() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testGetBerkleyEnvironment() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }

    @Test
    public void testStartup() throws Exception
    {
        System.out.println("testStartup :: begin");
        // Create mock objects.
        // BJEStorageImplementation doesn't require any other dependencies
        // to be injected before "startup" method call.
        HGStore mockStore = PowerMock.createStrictMock(HGStore.class);
        HGConfiguration mockConfiguration = PowerMock.createStrictMock(HGConfiguration.class);

        // Record expectations for mocked objects behaviour
        EasyMock.expect(mockConfiguration.getHandleFactory()).andReturn(
                (HGHandleFactory) Class.forName(HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME).newInstance());
        EasyMock.expect(mockConfiguration.isTransactional()).andReturn(true).times(2);
        EasyMock.expect(mockStore.getDatabaseLocation()).andReturn(TEMP_DATABASE_DIRECTORY);
        PowerMock.replayAll();

        // Test "startup" method
        BJEStorageImplementation storage = new BJEStorageImplementation();
        storage.startup(mockStore, mockConfiguration);

        // Verify mocked objects behaviour
        PowerMock.verifyAll();

        // Check whether the tested method performed correctly
        DatabaseConfig actualDatabaseConfig = storage.getConfiguration().getDatabaseConfig();
        Environment actualEnvironment = storage.getBerkleyEnvironment();
        assertTrue(actualDatabaseConfig.getTransactional(), "Storage should be transactional");
        assertFalse(actualDatabaseConfig.getReadOnly(), "Storage should be not readonly");
        assertTrue(actualEnvironment.isValid(), "Environment is not valid");
        String actualDatabaseLocation = actualEnvironment.getHome().getPath();
        assertEquals(actualDatabaseLocation, TEMP_DATABASE_DIRECTORY,
                String.format("Database location should be %s but %s is", TEMP_DATABASE_DIRECTORY, actualDatabaseLocation));
        System.out.println("testStartup :: OK");
    }
    // @Test
    // public void testShutdown() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testRemoveLink() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }

    // @Test
    // public void testStore() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testStore2() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testAddIncidenceLink() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testContainsLink() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testGetData() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testGetIncidenceResultSet() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testGetIncidenceSetCardinality() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testGetLink() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testGetTransactionFactory() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testRemoveData() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testRemoveIncidenceLink() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testRemoveIncidenceSet() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testGetIndex() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }
    //
    // @Test
    // public void testRemoveIndex() throws Exception
    // {
    // throw new IllegalStateException("Not yet implemented");
    // }

    // public static void main(String args[])
    // {
    // String databaseLocation = "/home/yura/hgdb/test";
    // HyperGraph graph = null;
    // try
    // {
    // graph = new HyperGraph(databaseLocation);
    // String text = "This is a test";
    // graph.add(text);
    // }
    // catch (Throwable t)
    // {
    // t.printStackTrace();
    // }
    // finally
    // {
    // graph.close();
    // }
    // }
}