import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.handle.LongHandleFactory;
import org.hypergraphdb.type.JavaTypeSchema;
import org.hypergraphdb.util.HGUtils;

public class Stub
{
	public static void main(String [] args)
	{
		String location = "/tmp/hgtestdeleme";
		HGUtils.dropHyperGraphInstance(location);
		HGConfiguration config = new HGConfiguration();
		((JavaTypeSchema)config.getTypeConfiguration().getDefaultSchema()).setPredefinedTypes("/org/hypergraphdb/types_intid");
		LongHandleFactory hgHandleFactory = new LongHandleFactory();
		config.setHandleFactory(hgHandleFactory);	
		HyperGraph graph = HGEnvironment.get(location, config);
		HGHandle h = graph.add("hi");
		graph.close();
		graph = HGEnvironment.get("/tmp/hgtestdeleme", config);
		System.out.println("Atom=" + graph.get(h));
	}
}