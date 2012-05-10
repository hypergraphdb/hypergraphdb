package hgtest;

import org.hypergraphdb.*;
import org.hypergraphdb.type.*;

/**
 * 
 * <p>
 * Test whether a record can refer to itself (i.e. have a slot whose value is the 
 * record itself).
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class CircRecord
{
	public static void main(String [] argv)
	{
		try
		{
			HyperGraph graph = HGEnvironment.get("c:/tmp/test");
			
			RecordType circType = new RecordType();
			Slot nameSlot = new Slot("testLabel", graph.getTypeSystem().getTypeHandle(String.class));
			circType.addSlot(graph.add(nameSlot));
			
			HGHandle recTypeHandle = graph.add(circType);		
			Slot parentSlot = new Slot("testParent", recTypeHandle);
			circType.addSlot(graph.add(parentSlot));
			graph.update(circType);
			
			Record rec = new Record(recTypeHandle);
			rec.set(nameSlot, "Guns'N'Roses");
			rec.set(parentSlot, rec);
			graph.add(rec, recTypeHandle);
			
			HGEnvironment.closeAll();
			System.out.println("Done.");
			System.exit(0);
		}
		catch (Throwable t)
		{
			t.printStackTrace(System.err);
			System.exit(-1);
		}		
	}
}