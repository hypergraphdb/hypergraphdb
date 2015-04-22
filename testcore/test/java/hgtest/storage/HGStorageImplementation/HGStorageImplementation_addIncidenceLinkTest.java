package hgtest.storage.HGStorageImplementation;

import hgtest.TestUtils;
import hgtest.storage.bdb.BDBStorageImplementation.BDBStorageImplementation_addIncidenceLinkTest;
import hgtest.storage.bdb.NativeLibrariesWorkaround;
import hgtest.storage.bje.BJEStorageImplementation.BJEStorageImplementation_addIncidenceLinkTest;
import com.google.code.multitester.annonations.ImportedTest;
import com.google.code.multitester.testers.MultiTester;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.hypergraphdb.util.HGUtils;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static hgtest.TestUtils.like2DArray;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuiy Sechko
 */
@PrepareForTest(HGConfiguration.class)
public class HGStorageImplementation_addIncidenceLinkTest extends
		PowerMockTestCase
{
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

	@Test(dataProvider = "configurations_2")
	public void addOneLink(final Class configuration) throws Exception
	{
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);
		storage.addIncidenceLink(first, second);

		final HGRandomAccessResult<HGPersistentHandle> storedLinks = storage
				.getIncidenceResultSet(first);
		assertEquals(storedLinks.next(), second);
		storedLinks.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_4")
	public void addSeveralLinks(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle(),
				new UUIDPersistentHandle() };

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);
		storage.addIncidenceLink(handle, links[0]);
		storage.addIncidenceLink(handle, links[1]);
		storage.addIncidenceLink(handle, links[2]);

		final HGRandomAccessResult<HGPersistentHandle> storedLinks = storage
				.getIncidenceResultSet(handle);
		assertEquals(TestUtils.set(storedLinks), HGUtils.set(links));
		storedLinks.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void addLinkToItself(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);
		storage.addIncidenceLink(handle, handle);

		final HGRandomAccessResult<HGPersistentHandle> storedLinks = storage
				.getIncidenceResultSet(handle);
		assertEquals(storedLinks.next(), handle);
		storedLinks.close();
		tester.shutdown();
	}

	@ImportedTest(testClass = BJEStorageImplementation_addIncidenceLinkTest.class, startupSequence = {
			"up1", "up_2" }, shutdownSequence = { "down2", "down1" })
	private static class BJE_HGStorageImplementation_2
	{
	}

	@ImportedTest(testClass = BJEStorageImplementation_addIncidenceLinkTest.class, startupSequence = {
			"up1", "up_4" }, shutdownSequence = { "down2", "down1" })
	private static class BJE_HGStorageImplementation_4
	{
	}

	@ImportedTest(testClass = BDBStorageImplementation_addIncidenceLinkTest.class, startupSequence = {
			"up1", "up_2" }, shutdownSequence = { "down2", "down1" })
	private static class BDB_HGStorageImplementation_2
	{
	}

	@ImportedTest(testClass = BDBStorageImplementation_addIncidenceLinkTest.class, startupSequence = {
			"up1", "up_4" }, shutdownSequence = { "down2", "down1" })
	private static class BDB_HGStorageImplementation_4
	{
	}
}
