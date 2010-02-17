package hgtest.workflow;

import org.hypergraphdb.peer.workflow.WorkflowState;

public class TestTransitions
{
    public static void main(String []argv)
    {
        WorkflowState state = WorkflowState.toStateConstant("Started");
        System.out.println("Workflow state " + state);
    }
}
