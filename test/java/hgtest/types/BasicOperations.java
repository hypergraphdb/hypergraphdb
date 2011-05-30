package hgtest.types;

import java.util.HashSet;
import java.util.concurrent.Callable;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSystemFlags;
import org.hypergraphdb.HyperGraph;
import hgtest.AtomOperation;
import hgtest.verify.Verifier;
import static org.testng.Assert.*;
import static hgtest.verify.HGAssert.*;

public class BasicOperations
{
    private HyperGraph graph;
    
    public BasicOperations(HyperGraph graph)
    {
        this.graph = graph;
    }
    
    public Verifier makeVerifier(final AtomOperation operation, boolean persistentHandles)
    {
        final HGHandle atomHandle = operation.atomHandle == null ? null : 
                              (persistentHandles ? operation.atomHandle.getPersistent() : operation.atomHandle);
        final HGHandle atomType = operation.atomType == null ? null : 
            (persistentHandles ? operation.atomType.getPersistent() : operation.atomType);        
        switch (operation.kind)
        {
            case add:
                return new Verifier()
                {
                    public void verify(HyperGraph graph)
                    {
                        assertEqualsDispatch(graph.get(atomHandle), operation.atomValue);
                        assertEqualsDispatch(graph.getType(atomHandle), atomType);
                    }
                };
            case remove:
                return new Verifier()
                {
                    public void verify(HyperGraph graph)
                    {
                        assertNull(graph.get(atomHandle));
                    }
                };
            case replace:
                return new Verifier()
                {
                    public void verify(HyperGraph graph)
                    {
                        assertEqualsDispatch(graph.get(atomHandle), operation.atomValue);
                        assertEqualsDispatch(graph.getType(atomHandle), atomType);
                    }
                };
            case define:
                return new Verifier()
                {
                    public void verify(HyperGraph graph)
                    {
                        assertEqualsDispatch(graph.get(atomHandle), operation.atomValue);
                        assertEqualsDispatch(graph.getType(atomHandle), atomType);
                    }
                };        
            default:
                throw new IllegalArgumentException("No operation kind.");
        }
    }
    
    public Verifier makeVerifier(final AtomOperation [] operations, final boolean persistentHandles)
    {
        return new Verifier()
        {
            HashSet<HGHandle> ignoreHandles = new HashSet<HGHandle>();
            public void verify(HyperGraph graph)
            {
                for (int i = operations.length - 1; i >= 0; i--)
                {
                    AtomOperation op = operations[i];
                    if (ignoreHandles.contains(op.atomHandle))
                        continue;
                    else
                        ignoreHandles.add(op.atomHandle);                        
                    makeVerifier(op, persistentHandles).verify(graph);
                }
            }
        };
    }
    
    public void execute(AtomOperation operation)
    {
        switch (operation.kind)
        {
            case add:
            {
                if (operation.atomType == null)
                {
                    operation.atomHandle = graph.add(operation.atomValue);
                    operation.atomType = graph.getType(operation.atomHandle);
                }
                else
                    operation.atomHandle = graph.add(operation.atomValue, operation.atomType);
                break;
            }
            case remove:
            {
                // what about the keepIncidentLinks?
                graph.remove(operation.atomHandle);
                break;
            }
            case replace:
            {
                if (operation.atomHandle != null)
                    if (operation.atomType == null)
                        graph.replace(operation.atomHandle, operation.atomValue);
                    else
                        graph.replace(operation.atomHandle, operation.atomValue, operation.atomType);
                else
                {
                    graph.update(operation.atomValue);
                    operation.atomHandle = graph.getHandle(operation.atomValue);
                }
                break;
            }
            case define:
            {
                HGPersistentHandle h = graph.getPersistentHandle(operation.atomHandle);
                if (operation.atomType != null)
                    graph.define(h, operation.atomType, operation.atomValue, HGSystemFlags.DEFAULT);
                else
                    graph.define(h, operation.atomValue, HGSystemFlags.DEFAULT);
            }
            break;
        }                
    }
    
    public void executeAndVerify(AtomOperation operation, boolean verifyCached, boolean verifyReloaded)
    {
        executeAndVerify(new AtomOperation[] { operation}, false, verifyCached, verifyReloaded);
    }
    
    public void executeAndVerify(final AtomOperation [] operations, 
                                 final boolean transact,
                                 final boolean verifyCached,
                                 final boolean verifyReloaded)
    {
        if (transact)
            graph.getTransactionManager().transact(new Callable<Object>()
            {
                public Object call()
                {
                    for (AtomOperation op : operations)
                        execute(op);
                    return null;
                }
            }
         );
        else for (AtomOperation op : operations)
            execute(op);
        if (verifyCached)
            makeVerifier(operations, false).verify(graph);
        if (verifyReloaded)
        {
            graph.close();
            graph.open(graph.getLocation());
            makeVerifier(operations, true).verify(graph);
        }
    }
}