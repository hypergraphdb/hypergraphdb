package hgtest.storage.bje.SingleValueResultSet;

import com.sleepycat.je.*;
import hgtest.storage.bje.ResultSetTestBasis;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.PlainSecondaryKeyCreator;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * In addition to the primary database SingleValueResultSet uses secondary
 * database. Navigation through second database is performed by SecondaryCursor.
 * This class contains code for setting-up secondary database and secondary
 * cursor. Most of the test cases uses this initialization code.
 *
 * Note: secondary cursor doesn't support putting data into database. But it
 * should be initialized before it can be used. In these cases the followed
 * approach is used:<br>
 * <ol>
 * <li>put data directly into secondary database</li>
 * <li>initialize secondary cursor by reading some dummy data</li>
 * </ol>
 * 
 * Calls for the BJETxCursor's methods are mocked, all real manipulations are
 * performed on Sleepycat library classes.
 * 
 * Only PlainSecondaryKeyCreator class is used in test, there are not fakes for
 * it.
 *
 * @author Yuriy Sechko
 */
public class SingleValueResultSetTestBasis extends ResultSetTestBasis
{
	protected SecondaryDatabase secondaryDatabase;
	protected SecondaryCursor realCursor;
	protected BJETxCursor fakeCursor;
	protected final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

	protected void startupEnvironment() throws Exception
	{
		super.startupEnvironment();
		final SecondaryConfig secondaryConfig = new SecondaryConfig();
		secondaryConfig.setAllowCreate(true).setReadOnly(false)
				.setTransactional(true);
		secondaryConfig.setKeyCreator(PlainSecondaryKeyCreator.getInstance());
		secondaryDatabase = environment.openSecondaryDatabase(
				transactionForTheEnvironment, DATABASE_NAME, database,
				secondaryConfig);
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

	protected void shutdownCursor()
	{
		realCursor.close();
	}

	/**
	 * All necessary fake calls before instance of SingleValueResultSet is about
	 * to constructed
	 */
	protected void createMocksForTheConstructor()
	{
		fakeCursor = PowerMock.createStrictMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
	}

	@BeforeMethod
	public void resetMocksAndDeleteTestDirectory() throws Exception
	{
		super.resetMocksAndDeleteTestDirectory();
		// startupEnvironment will be called from the super class automatically
		// (will be called exactly
		// SingleValueResultSetTestBasis.startupEnvironment() method)
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
