package hgtest.storage.bje.SingleValueResultSet;

import com.sleepycat.je.*;
import hgtest.storage.bje.ResultSetTestBasis;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.PlainSecondaryKeyCreator;
import org.hypergraphdb.storage.bje.SingleValueResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class SingleValueResultSet_goToWithExactMatchTest extends
		ResultSetTestBasis
{
	protected static final String SECONDARY_DATABASE_NAME = "test_database";
	private static final boolean EXACT_MATCH = true;

	protected SecondaryDatabase secondaryDatabase;
	protected SecondaryCursor realCursor;
	protected SingleValueResultSet<Integer> resultSet;

	final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

	@BeforeMethod
	public void resetMocksAndDeleteTestDirectory() throws Exception
	{
		super.resetMocksAndDeleteTestDirectory();
	}

	@AfterMethod
	public void verifyMocksAndDeleteTestDirectory() throws Exception
	{
		PowerMock.verifyAll();
		transactionForTheEnvironment.commit();
		secondaryDatabase.close();
		database.close();
		environment.close();
		TestUtils.deleteDirectory(envHome);
	}

	protected void startupEnvironment() throws Exception
	{
		super.startupEnvironment();
		final SecondaryConfig secondaryConfig = new SecondaryConfig();
		secondaryConfig.setAllowCreate(true).setReadOnly(false)
				.setTransactional(true);
		secondaryConfig.setKeyCreator(PlainSecondaryKeyCreator.getInstance());
		secondaryDatabase = environment.openSecondaryDatabase(
				transactionForTheEnvironment, SECONDARY_DATABASE_NAME,
				database, secondaryConfig);
	}

	protected void startupCursor()
	{
		realCursor = secondaryDatabase.openCursor(transactionForTheEnvironment,
				null);
		final DatabaseEntry stubKey = new DatabaseEntry();
		final DatabaseEntry stubValue = new DatabaseEntry();
		// initialize secondary cursor
		realCursor.getFirst(stubKey, stubValue, LockMode.DEFAULT);
	}

	protected void startupMocks()
	{
		final BJETxCursor fakeCursor = PowerMock
				.createStrictMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor).times(2);
		PowerMock.replayAll();
		resultSet = new SingleValueResultSet<Integer>(fakeCursor, null,
				converter);
	}

	protected void shutdownCursor()
	{
		realCursor.close();
	}

	protected void putKeyValuePair(final Database database, final Integer key,
			final Integer value)
	{
		final Transaction transactionForAddingTestData = environment
				.beginTransaction(null, null);
		database.put(
				transactionForTheEnvironment,
				new DatabaseEntry(new TestUtils.ByteArrayConverterForInteger()
						.toByteArray(key)),
				new DatabaseEntry(new TestUtils.ByteArrayConverterForInteger()
						.toByteArray(value)));
		transactionForAddingTestData.commit();
	}

	@Test
	public void valueIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		// Secondary cursor should be initialized, but it doesn't support
		// putting data. So put some data into primary database before
		// starting up the secondary cursor
		putKeyValuePair(database, 1, 11);
		startupCursor();
		final BJETxCursor fakeCursor = PowerMock
				.createStrictMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 0, 0, 0, 0 });
		final SingleValueResultSet<Integer> resultSet = new SingleValueResultSet<Integer>(
				fakeCursor, key, converter);

		try
		{
			resultSet.goTo(null, EXACT_MATCH);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			shutdownCursor();
		}
	}

	@Test
	public void thereIsOneValueButItIsLessThanDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		putKeyValuePair(database, 1, 10);
		startupCursor();
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(20,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereIsOneValueButItIsGreaterThanDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		putKeyValuePair(database, 1, 10);
		startupCursor();
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(9,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereIsOneValueAndItIsEqualToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		putKeyValuePair(database, 1, 10);
		startupCursor();
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(10,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}
}
