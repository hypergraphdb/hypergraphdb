package hgtest.storage.bje.LinkBinding;

import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.storage.bje.LinkBinding;
import org.junit.Test;

public class LinkBinding_constructorTest extends LinkBindingTestBasis
{
	@Test
	public void throwsException_whenHandleFactoryIsNull() throws Exception
	{
		below.expect(NullPointerException.class);
		new LinkBinding(null);
	}

	@Test
	public void happyPath() throws Exception
	{
		final HGHandleFactory handleFactory = (HGHandleFactory) Class.forName(
				HANDLE_FACTORY_CLASS_NAME).newInstance();

		new LinkBinding(handleFactory);
	}
}
