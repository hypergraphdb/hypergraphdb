package hgtest.storage.bje.BJEConfig;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.EnvironmentConfig;
import org.hypergraphdb.storage.bje.BJEConfig;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.testng.Assert.assertEquals;

/**
 * Test cases for getters in {@link org.hypergraphdb.storage.bje.BJEConfig}
 *
 * @author Yuriy Sechko
 */
public class BJEConfig_getConfigTest
{
	@Test
	public void getEnvironmentConfigTest() throws Exception
	{
		final EnvironmentConfig expected = new EnvironmentConfig();

		final BJEConfig bjeConfig = new BJEConfig();
		final Field privateConfigField = bjeConfig.getClass().getDeclaredField(
				"envConfig");
		privateConfigField.setAccessible(true);
		privateConfigField.set(bjeConfig, expected);

		final EnvironmentConfig actual = bjeConfig.getEnvironmentConfig();

		// equals() is not overridden in EnvironmentConfig class. So not sure
		// about its behavior.
		// But toString is overridden. It is pretty enough for comparing objects
		// in the present test case.
		assertEquals(actual.toString(), expected.toString());
	}

	@Test
	public void getDatabaseConfigTest() throws Exception
	{
		final DatabaseConfig expected = new DatabaseConfig();
		final BJEConfig bjeConfig = new BJEConfig();
		final Field privateConfigField = bjeConfig.getClass().getDeclaredField(
				"dbConfig");
		privateConfigField.setAccessible(true);
		privateConfigField.set(bjeConfig, expected);

		final DatabaseConfig actual = bjeConfig.getDatabaseConfig();

		// see note above
		assertEquals(actual.toString(), expected.toString());
	}
}
