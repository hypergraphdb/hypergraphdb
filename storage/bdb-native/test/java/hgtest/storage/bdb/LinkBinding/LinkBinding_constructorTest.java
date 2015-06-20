package hgtest.storage.bdb.LinkBinding;

import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.storage.bdb.LinkBinding;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class LinkBinding_constructorTest extends LinkBindingTestBasis
{
	@Test
	public void handleFactoryIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		try
		{
			new LinkBinding(null);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void handleFactoryIsNotNull() throws Exception
	{
		final HGHandleFactory handleFactory = (HGHandleFactory) Class.forName(
				HANDLE_FACTORY_CLASS_NAME).newInstance();

		new LinkBinding(handleFactory);
	}
}
