package hgtest.storage.HGStorageImplementation;

import com.google.code.multitester.testers.MultiTester;
import hgtest.TestUtils;
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
import static org.testng.Assert.assertFalse;

/**
 * @author Yuriy Sechko
 */
@PrepareForTest(HGConfiguration.class)
public class HGStorageImplementation_getIncidenceResultSetTest extends
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
	public void noIncidenceLinksAreStored(final Class configuration)
			throws Exception
	{
		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);

		final HGPersistentHandle handle = new UUIDPersistentHandle();

		final HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(handle);

		assertFalse(incidence.hasNext());
		incidence.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void oneIncidenceLinkIsStored(final Class configuration)
			throws Exception
	{
		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);

		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);

		final HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(first);

		assertEquals(incidence.next(), second);
		incidence.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_4")
	public void severalIncidenceLinksAreStored(final Class configuration)
			throws Exception
	{
		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);

		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle(),
				new UUIDPersistentHandle() };
		storage.addIncidenceLink(first, links[0]);
		storage.addIncidenceLink(first, links[1]);
		storage.addIncidenceLink(first, links[2]);

		final HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(first);

		assertEquals(TestUtils.set(incidence), HGUtils.set(links));
		incidence.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void checkLinksFromSecondToFirst(final Class configuration)
			throws Exception
	{
		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);

		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);

		final HGRandomAccessResult<HGPersistentHandle> incidenceFromSecondToFirst = storage
				.getIncidenceResultSet(second);

		assertFalse(incidenceFromSecondToFirst.hasNext());
		incidenceFromSecondToFirst.close();
		tester.shutdown();
	}

	@Test(dataProvider = "configurations_2")
	public void handleIsLinkedToItself(final Class configuration)
			throws Exception
	{
		final MultiTester tester = new MultiTester(configuration);
		tester.startup();
		final HGStoreImplementation storage = tester.importField("underTest",
				HGStoreImplementation.class);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, handle);

		HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(handle);

		assertEquals(incidence.next(), handle);
		incidence.close();
		tester.shutdown();
	}
}
