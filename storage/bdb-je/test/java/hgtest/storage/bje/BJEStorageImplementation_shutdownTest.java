package hgtest.storage.bje;

import com.sleepycat.je.Environment;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

/**
 */
public class BJEStorageImplementation_shutdownTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void getDatabasePathAfterShutdown() throws Exception
	{
		startup();
		storage.shutdown();
		final Environment environment = storage.getBerkleyEnvironment();
		try
		{
			// environment is not open, expect exception
			environment.getHome().getPath();
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), IllegalStateException.class);
			assertEquals(ex.getMessage(),
					"Attempt to use non-open Environment object().");
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
