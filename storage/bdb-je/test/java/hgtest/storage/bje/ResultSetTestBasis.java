package hgtest.storage.bje;

import com.sleepycat.je.*;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;

/**
 * Contains common code for test cases for legatees of
 * {@link org.hypergraphdb.storage.bje.IndexResultSet}.
 *
 * @author Yuriy Sechko
 */
@PrepareForTest(BJETxCursor.class)
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
