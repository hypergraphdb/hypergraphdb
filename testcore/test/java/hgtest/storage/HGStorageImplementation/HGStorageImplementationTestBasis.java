package hgtest.storage.HGStorageImplementation;

import com.google.code.multitester.annonations.ImportedTest;
import com.google.code.multitester.testers.MultiTester;
import hgtest.storage.bdb.BDBStorageImplementation.BDBStorageImplementationTestBasis;
import hgtest.storage.bje.BJEStorageImplementation.BJEStorageImplementationTestBasis;
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
	@DataProvider(name = "configurations_0")
	public Object[][] provide0() throws Exception
	{
		return like2DArray(Configurations.BJE_HGStorageImplementation_0.class,
				Configurations.BDB_HGStorageImplementation_0.class);
	}

	@DataProvider(name = "configurations_1")
	public Object[][] provide1() throws Exception
	{
		return like2DArray(Configurations.BJE_HGStorageImplementation_1.class,
				Configurations.BDB_HGStorageImplementation_1.class);
	}

	@DataProvider(name = "configurations_2")
	public Object[][] provide2() throws Exception
	{
		return like2DArray(Configurations.BJE_HGStorageImplementation_2.class,
				Configurations.BDB_HGStorageImplementation_2.class);
	}

	@DataProvider(name = "configurations_3")
	public Object[][] provide3() throws Exception
	{
		return like2DArray(Configurations.BJE_HGStorageImplementation_3.class,
				Configurations.BDB_HGStorageImplementation_3.class);
	}

	@DataProvider(name = "configurations_4")
	public Object[][] provide4() throws Exception
	{
		return like2DArray(Configurations.BJE_HGStorageImplementation_4.class,
				Configurations.BDB_HGStorageImplementation_4.class);
	}

	@DataProvider(name = "configurations_transaction_3")
	public Object[][] provide_transaction_3() throws Exception
	{
		return like2DArray(
				Configurations.BJE_HGStorageImplementation_transaction_3.class,
				Configurations.BDB_HGStorageImplementation_transaction_3.class);
	}

	@DataProvider(name = "configurations_transaction_5")
	public Object[][] provide_transaction_5() throws Exception
	{
		return like2DArray(
				Configurations.BJE_HGStorageImplementation_transaction_5.class,
				Configurations.BDB_HGStorageImplementation_transaction_5.class);
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

	// Placeholder for test cases configurations.
	public static class Configurations
	{
		// No calls to transaction manager
		@ImportedTest(testClass = BJEStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_0" }, shutdownSequence = { "down2", "down1" })
		public static class BJE_HGStorageImplementation_0
		{
		}

		@ImportedTest(testClass = BDBStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_0" }, shutdownSequence = { "down2", "down1" })
		public static class BDB_HGStorageImplementation_0
		{
		}

		// one call
		@ImportedTest(testClass = BJEStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_1" }, shutdownSequence = { "down2", "down1" })
		public static class BJE_HGStorageImplementation_1
		{
		}

		@ImportedTest(testClass = BDBStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_1" }, shutdownSequence = { "down2", "down1" })
		public static class BDB_HGStorageImplementation_1
		{
		}

		// two calls
		@ImportedTest(testClass = BJEStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_2" }, shutdownSequence = { "down2", "down1" })
		public static class BJE_HGStorageImplementation_2
		{
		}

		@ImportedTest(testClass = BDBStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_2" }, shutdownSequence = { "down2", "down1" })
		public static class BDB_HGStorageImplementation_2
		{
		}

		// three calls
		@ImportedTest(testClass = BJEStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_3" }, shutdownSequence = { "down2", "down1" })
		public static class BJE_HGStorageImplementation_3
		{
		}

		@ImportedTest(testClass = BDBStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_3" }, shutdownSequence = { "down2", "down1" })
		public static class BDB_HGStorageImplementation_3
		{
		}

		// four calls
		@ImportedTest(testClass = BJEStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_4" }, shutdownSequence = { "down2", "down1" })
		public static class BJE_HGStorageImplementation_4
		{
		}

		@ImportedTest(testClass = BDBStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_4" }, shutdownSequence = { "down2", "down1" })
		public static class BDB_HGStorageImplementation_4
		{
		}

		// Configurations for startup with additional transactions.

		// three additional transactions
		@ImportedTest(testClass = BJEStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_t_3" }, shutdownSequence = { "down2", "down1" })
		public static class BJE_HGStorageImplementation_transaction_3
		{
		}

		@ImportedTest(testClass = BDBStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_t_3" }, shutdownSequence = { "down2", "down1" })
		public static class BDB_HGStorageImplementation_transaction_3
		{
		}

		// five additional transactions
		@ImportedTest(testClass = BJEStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_t_5" }, shutdownSequence = { "down2", "down1" })
		public static class BJE_HGStorageImplementation_transaction_5
		{
		}

		@ImportedTest(testClass = BDBStorageImplementationTestBasis.class, startupSequence = {
				"up1", "up_t_5" }, shutdownSequence = { "down2", "down1" })
		public static class BDB_HGStorageImplementation_transaction_5
		{
		}
	}
}