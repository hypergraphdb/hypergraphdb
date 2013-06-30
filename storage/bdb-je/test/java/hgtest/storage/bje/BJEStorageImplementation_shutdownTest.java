package hgtest.storage.bje;

import com.sleepycat.je.Environment;
import org.testng.annotations.Test;

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
}
