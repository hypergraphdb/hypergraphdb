package hgtest.storage.bje.LinkBinding;

import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.storage.bje.LinkBinding;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static hgtest.storage.bje.LinkBinding.LinkBindingTestBasis.HANDLE_FACTORY_CLASS_NAME;

/**
 * @author Yuriy Sechko
 */
public class LinkBinding_constructorTest
{
	@Test
	public void handleFactoryIsNull() throws Exception
	{
		try
		{
			new LinkBinding(null);
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), NullPointerException.class);
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
