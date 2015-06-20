package hgtest.storage.bdb.SingleValueResultSet;

import com.sleepycat.db.*;
import hgtest.storage.bdb.ResultSetTestBasis;
import hgtest.storage.bdb.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bdb.BDBTxCursor;
import org.hypergraphdb.storage.bdb.PlainSecondaryKeyCreator;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * @author Yuriy Sechko
 */
public class SingleValueResultSetTestBasis extends ResultSetTestBasis
{
	protected static final String SECONDARY_DATABASE_NAME = DATABASE_NAME
			+ "_secondary";
	public static final String DATABASE_FILE_NAME = "bdb-native-database-data";

	protected SecondaryDatabase secondaryDatabase;
	protected Cursor realCursor;
	protected BDBTxCursor fakeCursor;
	protected final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

	protected void startupEnvironment() throws Exception
	{
		super.startupEnvironment();
		final SecondaryConfig secondaryConfig = new SecondaryConfig();
		secondaryConfig.setAllowCreate(true);
		secondaryConfig.setType(DatabaseType.BTREE);
		secondaryConfig.setKeyCreator(PlainSecondaryKeyCreator.getInstance());
		secondaryDatabase = environment.openSecondaryDatabase(
				transactionForTheEnvironment, SECONDARY_DATABASE_NAME,
				DATABASE_FILE_NAME, database, secondaryConfig);
	}

	protected void startupCursor() throws Exception
	{
		realCursor = secondaryDatabase.openCursor(transactionForTheEnvironment,
				null);
		final DatabaseEntry stubKey = new DatabaseEntry();
		final DatabaseEntry stubValue = new DatabaseEntry();
		// initialize secondary cursor
		realCursor.getFirst(stubKey, stubValue, LockMode.DEFAULT);
	}

	protected void shutdownCursor() throws Exception
	{
		realCursor.close();
	}

	protected void createMocksForTheConstructor()
	{
		fakeCursor = PowerMock.createStrictMock(BDBTxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
	}

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
}
