package hgtest.storage.bdb;

import com.sleepycat.db.*;
import org.hypergraphdb.storage.bdb.BDBTxCursor;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;

/**
 * Contains common code for test cases for legatees of
 * {@link org.hypergraphdb.storage.bdb.IndexResultSet}.
 *
 * @author Yuriy Sechko
 */
@PrepareForTest(BDBTxCursor.class)
public class ResultSetTestBasis extends PowerMockTestCase
{
	protected static final String DATABASE_NAME = "test_database";

	protected final File envHome = TestUtils.createTempFile("IndexImpl",
            "test_environment");
	protected Environment environment;
	protected Database database;
	protected Transaction transactionForTheEnvironment;

	protected void startupEnvironment() throws Exception
	{
		NativeLibrariesWorkaround.loadNativeLibraries();
		envHome.mkdir();
		final EnvironmentConfig environmentConfig = new EnvironmentConfig();
		environmentConfig.setAllowCreate(true);
		environmentConfig.setTransactional(true);
		environmentConfig.setInitializeCache(true);
		environment = new Environment(envHome, environmentConfig);
		transactionForTheEnvironment = environment.beginTransaction(null, null);
		final DatabaseConfig databaseConfig = new DatabaseConfig();
		databaseConfig.setAllowCreate(true);
		databaseConfig.setTransactional(true);
		databaseConfig.setType(DatabaseType.BTREE);
		database = environment.openDatabase(transactionForTheEnvironment,
				DATABASE_NAME, null, databaseConfig);
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
		PowerMock.verifyAll();
		transactionForTheEnvironment.commit();
		database.close();
		environment.close();
		TestUtils.deleteDirectory(envHome);
	}
}
