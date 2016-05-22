package hgtest.storage.bje.BJEStorageImplementation;

import java.lang.reflect.Field;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Environment;

public class BJEStorageImplementation_shutdownTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void throwsException_whenManipulationIsPerformedAfterShutdown()
			throws Exception
	{
		storage.shutdown();

		final Environment environment = storage.getBerkleyEnvironment();

		below.expect(IllegalStateException.class);
		below
				.expectMessage("Attempt to use non-open Environment object().");
		environment.getHome().getPath();
	}

	@Test
	public void doesNotFail_whenCheckPointThreadBecomesNull() throws Exception
	{
		// set value of field checkPointThread in
		// BJEStorageImplementation instance to null
		final Field checkPointThread = storage.getClass().getDeclaredField(
				"checkPointThread");
		checkPointThread.setAccessible(true);
		checkPointThread.set(storage, null);
		shutdown();
	}

	@Test
	public void doesNotFails_whenEnvBecomesNull() throws Exception
	{
		// set value of field env in BJEStorageImplementation instance to null
		final Field env = storage.getClass().getDeclaredField("env");
		env.setAccessible(true);
		env.set(storage, null);
		shutdown();
	}

    @Before
    public void startup() throws Exception {
        super.startup();
    }
}
