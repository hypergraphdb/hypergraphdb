package hgtest.storage.bje.KeyScanResultSet;

import com.sleepycat.je.*;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
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
public class KeyScanResultSet_constructorTest extends PowerMockTestCase
{
	protected static final String DATABASE_NAME = "test_database";

	protected final File envHome = TestUtils.createTempFile("IndexImpl",
			"test_environment");
	protected Environment environment;
	protected Database database;
	protected Transaction transaction;

	protected void startupEnvironment() throws Exception
	{
		envHome.mkdir();
		final EnvironmentConfig environmentConfig = new EnvironmentConfig();
		environmentConfig.setAllowCreate(true).setReadOnly(false)
				.setTransactional(true);
		environment = new Environment(envHome, environmentConfig);
		transaction = environment.beginTransaction(null, null);
		final DatabaseConfig databaseConfig = new DatabaseConfig();
		databaseConfig.setAllowCreate(true).setTransactional(true);
		database = environment.openDatabase(transaction, DATABASE_NAME,
				databaseConfig);
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
		transaction.commit();
		database.close();
		environment.close();
		TestUtils.deleteDirectory(envHome);
	}

	@Test
	public void cursorIsNull() throws Exception
	{
		final Exception expected = new HGException(
				"java.lang.NullPointerException");

		final DatabaseEntry keyIndex = new DatabaseEntry(new byte[] { 0, 0, 0,
				0 });
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
		PowerMock.replayAll();

		try
		{
			new KeyScanResultSet<Integer>(null, keyIndex, converter);
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
		}
	}

	@Test
	public void keyIndexIsNull() throws Exception
	{
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
		final Transaction transactionForTheRealCursor = environment
				.beginTransaction(null, null);
		final Cursor realCursor = database.openCursor(transaction, null);
		realCursor.put(new DatabaseEntry(new byte[] { 0, 1, 2, 3 }),
				new DatabaseEntry(new byte[] { 0, 1, 2, 3 }));
		final BJETxCursor fakeCursor = PowerMock.createMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
		PowerMock.replayAll();

		new KeyScanResultSet<Integer>(fakeCursor, null, converter);

		realCursor.close();
		transactionForTheRealCursor.commit();
	}

	@Test
	public void bjeCursorThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"java.lang.IllegalStateException: This exception is thrown by fake cursor.");

		final DatabaseEntry keyIndex = new DatabaseEntry(new byte[] { 0, 0, 0,
				0 });
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
		final BJETxCursor fakeCursor = PowerMock.createMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andThrow(
				new IllegalStateException(
						"This exception is thrown by fake cursor."));
		PowerMock.replayAll();

		try
		{
			new KeyScanResultSet<Integer>(fakeCursor, keyIndex, converter);
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
		}
	}
}
