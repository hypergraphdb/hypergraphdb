package hgtest.storage.bje.BJEStorageImplementation;

import com.sleepycat.je.Environment;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_shutdownTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void getDatabasePathAfterShutdown() throws Exception
	{
		final Exception expected = new IllegalStateException(
				"Attempt to use non-open Environment object().");

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
		finally
		{
			shutdown();
		}
	}

	@Test
	public void checkPointThreadIsNull() throws Exception
	{
		startup();
		// set value of field checkPointThread in
		// BJEStorageImplementation instance to null
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
		// set value of field env in BJEStorageImplementation instance to null
		final Field env = storage.getClass().getDeclaredField("env");
		env.setAccessible(true);
		env.set(storage, null);
		shutdown();
	}
}
