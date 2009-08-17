package org.hypergraphdb.maintenance;

import java.util.ArrayList;
import java.util.List;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGRandomAccessResult.GotoResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.indexing.HGValueIndexer;
import org.hypergraphdb.util.HGUtils;

/**
 * 
 * <p>
 * The <code>MaintenanceOperation</code> will create index entries for a newly
 * added <code>HGIndexer</code>.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
@SuppressWarnings("unchecked")
public class ApplyNewIndexer implements MaintenanceOperation
{
	private HGHandle hIndexer;
	private List<HGHandle> typesAdded = new ArrayList<HGHandle>();
	private HGPersistentHandle lastProcessed = null;
	private int batchSize = 100;
	
	private void cleanupAfterFailure(HyperGraph graph, HGIndexer indexer, MaintenanceException ex)
	{
		try
		{
			graph.getIndexManager().unregister(indexer); // this will delete all existing entries
			graph.getIndexManager().register(indexer);
		}
		catch (Throwable t)
		{
			ex.setFatal(true);
		}
	}
	
	private void indexAtomsTypedWith(HyperGraph graph,
	                                 HGIndex idx,
	                                 HGIndexer indexer, 
	                                 HGHandle typeHandle) throws MaintenanceException
	{
        HGRandomAccessResult<HGPersistentHandle> rs = null;
        while (true)
        {
            graph.getTransactionManager().beginTransaction();           
            try
            {
                rs = (HGRandomAccessResult<HGPersistentHandle>)(HGRandomAccessResult)
                        graph.find(hg.type(typeHandle));
                if (lastProcessed == null)
                {
                    if (!rs.hasNext()) 
                    {
                        graph.getTransactionManager().endTransaction(false);
                        return; 
                    }
                    else rs.next();
                }
                else
                {
                    GotoResult gt = rs.goTo(lastProcessed, false);
                    if (gt == GotoResult.nothing) // last processed was actually last element in result set
                    {
                        graph.getTransactionManager().endTransaction(false);                        
                        return;
                    }
                    else if (gt == GotoResult.found)
                    {
                        if (!rs.hasNext())
                        {
                            graph.getTransactionManager().endTransaction(false);                            
                            return;
                        }
                        else
                            rs.next();
                    } // else we are already positioned after the last processed, which is not present for god know why?
                }               
                for (int i = 0; i < batchSize; i++)
                {
                    Object atom = graph.get(rs.current());
                    if (indexer instanceof HGValueIndexer)
                        idx.addEntry(indexer.getKey(graph, atom), ((HGValueIndexer)indexer).getValue(graph, atom)); 
                    else
                        idx.addEntry(indexer.getKey(graph, atom), rs.current());
                    lastProcessed = rs.current();                   
                    if (!rs.hasNext())
                        break;
                    else
                        rs.next();
                }
                rs.close();
                rs = null;
                graph.update(this);
                graph.getTransactionManager().endTransaction(true);
            }
            catch (Throwable t)
            {
                try { graph.getTransactionManager().endTransaction(false); }
                catch (Throwable tt) { tt.printStackTrace(System.err); }
                MaintenanceException mex = new MaintenanceException(
                    false, 
                    "While creating populating index for indexer : " + indexer,
                    t);
                cleanupAfterFailure(graph, indexer, mex);
                throw mex;
            }
            finally
            {
                HGUtils.closeNoException(rs);                
            }                   
        }	    
	}
	
	public ApplyNewIndexer()
	{		
	}
	
	public ApplyNewIndexer(HGHandle hIndexer)
	{
		this.hIndexer = hIndexer;
	}
	
	public void execute(HyperGraph graph) throws MaintenanceException
	{		
		HGIndexer indexer = graph.get(hIndexer);
		if (indexer == null)
			return;
		HGIndex idx = graph.getIndexManager().getIndex(indexer);
		if (idx == null)
			throw new MaintenanceException(false,"Indexer " + indexer + " with handle " + hIndexer + 
												 " present in graph, but no actual index has been created.");
		for (HGHandle currentType : hg.typePlus(indexer.getType()).getSubTypes(graph))
		{		    
		    if (typesAdded.contains(currentType)) // are we resuming from a previous interruption?
		    {
		        // if the type has been completed processed, skip it
		        if (!typesAdded.get(typesAdded.size()-1).equals(currentType))
		            continue;
		        // otherwise, we are resuming the processing of 'currentType' and
		        // the lastProcessed variable should contain the handle of the last
		        // atom of that type that was added to the index
		    }
		    else
		    {
		        typesAdded.add(currentType);
                lastProcessed = null;
		    }
		    indexAtomsTypedWith(graph, idx, indexer, currentType);		    
		}
	}

	public HGHandle getHIndexer()
	{
		return hIndexer;
	}

	public void setHIndexer(HGHandle indexer)
	{
		hIndexer = indexer;
	}

	public HGPersistentHandle getLastProcessed()
	{
		return lastProcessed;
	}

	public void setLastProcessed(HGPersistentHandle lastProcessed)
	{
		this.lastProcessed = lastProcessed;
	}

	public int getBatchSize()
	{
		return batchSize;
	}

	public void setBatchSize(int batchSize)
	{
		this.batchSize = batchSize;
	}		
}