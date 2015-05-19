package hgtest.storage.HGStorageImplementation;

import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;

import static hgtest.TestUtils.like2DArray;
import static org.testng.Assert.fail;

/**
 * @author Yuriy Sechko
 */
@PrepareForTest(HGConfiguration.class)
public class HGStorageImplementationTestBasis extends PowerMockTestCase
{
    // Configuration providers (TestNG's feature).
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

    // Helper code to reduce amount of initialization code.
    protected MultiTester tester;
    protected HGStoreImplementation storage;

    protected void initSpecificStorageImplementation(final Class configuration)
    {
        this.tester = new MultiTester(configuration);
        tester.startup();
        this.storage = tester.importField("underTest",
                HGStoreImplementation.class);
    }

    @AfterMethod
	public void shutdownTestCaseViaTester()
	{
		if (this.tester == null || this.storage == null)
			fail("Storage is not initialized in this test case.");
		this.tester.shutdown();
	}
}