package hgtest.storage.bdb.BDBStorageImplementation;

import com.sleepycat.db.Environment;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BDBStorageImplementation_shutdownTest extends
		BDBStorageImplementationTestBasis
{
	@Test
	public void getDatabasePathAfterShutdown() throws Exception
	{
		final Exception expected = new IllegalArgumentException(
				"call on closed handle");

		startup();
		storage.shutdown();
		final Environment environment = storage.getBerkleyEnvironment();
		try
		{
			// environment is not open, expect exception
			environment.getHome().getPath();
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void checkPointThreadIsNull() throws Exception
	{
		startup();
		// set value of field 'checkPointThread' in
		// BDBStorageImplementation instance to null
		final Field checkPointThread = storage.getClass().getDeclaredField(
				"checkPointThread");
		checkPointThread.setAccessible(true);
		checkPointThread.set(storage, null);
		shutdown();
	}

	@Test
	public void envIsNull() throws Exception
	{
		startup();
		// set value of field 'env' in BDBStorageImplementation instance to null
		final Field env = storage.getClass().getDeclaredField("env");
		env.setAccessible(true);
		env.set(storage, null);
		shutdown();
	}
}
