package hgtest.storage.bje.KeyScanResultSet;

import com.sleepycat.je.*;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
@PrepareForTest(BJETxCursor.class)
public class KeyScanResultSet_goToWithExactMatchTest extends PowerMockTestCase
{

	protected static final String DATABASE_NAME = "test_database";
	protected static final boolean EXACT_MATCH = true;

	protected final File envHome = TestUtils.createTempFile("IndexImpl",
			"test_environment");
	protected Environment environment;
	protected Database database;
	protected Transaction transactionForTheEnvironment;
	protected Cursor realCursor;
	protected Transaction transactionForTheRealCursor;

	protected final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
	protected KeyScanResultSet<Integer> keyScan;

	protected void startupEnvironment() throws Exception
	{
		envHome.mkdir();
		final EnvironmentConfig environmentConfig = new EnvironmentConfig();
		environmentConfig.setAllowCreate(true).setReadOnly(false)
				.setTransactional(true);
		environment = new Environment(envHome, environmentConfig);
		transactionForTheEnvironment = environment.beginTransaction(null, null);
		final DatabaseConfig databaseConfig = new DatabaseConfig();
		databaseConfig.setAllowCreate(true).setTransactional(true);
		database = environment.openDatabase(transactionForTheEnvironment,
				DATABASE_NAME, databaseConfig);
		transactionForTheRealCursor = environment.beginTransaction(null, null);
		realCursor = database.openCursor(transactionForTheEnvironment, null);
	}

	protected void startupMocks()
	{
		final BJETxCursor fakeCursor = PowerMock.createMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor).times(2);
		PowerMock.replayAll();
		keyScan = new KeyScanResultSet<Integer>(fakeCursor, null, converter);
	}

	protected void shutdownEnvironment()
	{
		realCursor.close();
		transactionForTheRealCursor.commit();
	}

	@BeforeMethod
	public void resetMocksAndDeleteTestDirectory() throws Exception
	{
		PowerMock.resetAll();
		TestUtils.deleteDirectory(envHome);
		startupEnvironment();
	}

	@AfterMethod
	public void verifyMocksAndDeleteTestDirectory() throws Exception
	{
		shutdownEnvironment();
		PowerMock.verifyAll();
		transactionForTheEnvironment.commit();
		database.close();
		environment.close();
		TestUtils.deleteDirectory(envHome);
	}

	protected void putKeyValuePair(Cursor realCursor, final Integer key,
			final String value)
	{
		realCursor.put(
				new DatabaseEntry(new TestUtils.ByteArrayConverterForInteger()
						.toByteArray(key)),
				new DatabaseEntry(new TestUtils.ByteArrayConverterForString()
						.toByteArray(value)));
	}

	@Test
	public void thereIsOneKeyButItIsNotEqualToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		putKeyValuePair(realCursor, 1, "one");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2, true);

		assertEquals(actual, expected);
	}

	@Test
	public void thereOneKeyButItIsEqualToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		putKeyValuePair(realCursor, 1, "one");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(1,
				EXACT_MATCH);

		assertEquals(actual, expected);
	}

	@Test
	public void thereOneKeyButItIsCloseToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		putKeyValuePair(realCursor, 1, "one");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
				EXACT_MATCH);

		assertEquals(actual, expected);
	}

	@Test
	public void thereAreTwoItemsButThereIsNotDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(-2,
				EXACT_MATCH);

		assertEquals(actual, expected);
	}

	@Test
	public void thereAreTwoItemsAndOnOfThemIsCloseToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 3, "three");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
				EXACT_MATCH);

		assertEquals(actual, expected);
	}

	@Test
	public void thereAreTwoItemsAndOnOfThemIsEqualToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
				EXACT_MATCH);

		assertEquals(actual, expected);
	}

	@Test
	public void thereAreThreeItemsButThereIsNotDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		putKeyValuePair(realCursor, 3, "three");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(5,
				EXACT_MATCH);

		assertEquals(actual, expected);
	}

	@Test
	public void thereAreThreeItemsAndOneOfThemIsEqualToDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		putKeyValuePair(realCursor, 3, "three");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
				EXACT_MATCH);

		assertEquals(actual, expected);
	}

	@Test
	public void thereAreThreeItemsAndSeveralOfThemAreEqualToDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		putKeyValuePair(realCursor, 3, "three");
		putKeyValuePair(realCursor, 3, "III");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(3,
				EXACT_MATCH);
		assertEquals(actual, expected);
	}
}
