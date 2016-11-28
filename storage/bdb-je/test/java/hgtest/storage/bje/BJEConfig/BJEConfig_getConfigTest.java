package hgtest.storage.bje.BJEConfig;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;

import org.hypergraphdb.storage.bje.BJEConfig;
import org.junit.Test;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.EnvironmentConfig;

/**
 * Test cases for getters in {@link org.hypergraphdb.storage.bje.BJEConfig}
 */
public class BJEConfig_getConfigTest
{
	@Test
	public void getEnvironmentConfig() throws Exception
	{
		final EnvironmentConfig expected = new EnvironmentConfig();

		final BJEConfig bjeConfig = new BJEConfig();
		final Field privateConfigField = bjeConfig.getClass().getDeclaredField(
				"envConfig");
		privateConfigField.setAccessible(true);
		privateConfigField.set(bjeConfig, expected);

		final EnvironmentConfig actual = bjeConfig.getEnvironmentConfig();

		assertEquals(expected.toString(), actual.toString());
	}

	@Test
	public void getDatabaseConfig() throws Exception
	{
		final DatabaseConfig expected = new DatabaseConfig();

		final BJEConfig bjeConfig = new BJEConfig();
		final Field privateConfigField = bjeConfig.getClass().getDeclaredField(
				"dbConfig");
		privateConfigField.setAccessible(true);
		privateConfigField.set(bjeConfig, expected);

		final DatabaseConfig actual = bjeConfig.getDatabaseConfig();

		assertEquals(expected.toString(), actual.toString());
	}
}
