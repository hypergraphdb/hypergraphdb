package org.hypergraphdb.peer.workflow;

import static org.hypergraphdb.peer.Messages.CONTENT;
import static org.hypergraphdb.peer.Messages.createMessage;
import static org.hypergraphdb.peer.Messages.getReply;
import static org.hypergraphdb.peer.Messages.getSender;
import static org.hypergraphdb.peer.Structs.combine;
import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.struct;

import java.util.UUID;

import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.SubgraphManager;

/**
 * <p>
 * An abstract activity implementing a simple, single round-trip, request
 * response activity where one peer acts as a client and another as a server.
 * </p>
 * 
 * <p>
 * Subclasses need to implement the {@link doRequest} method to handle an
 * incoming <code>Request</code> and return an appropriate <code>Response</code>.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public abstract class RequestResponseActivity<Request, Response> extends FSMActivity
{
    private HGPeerIdentity target;
    private Request request;
    private Response response;

    protected abstract Response doRequest(Request request);
        
    public RequestResponseActivity(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public RequestResponseActivity(HyperGraphPeer thisPeer, UUID id, HGPeerIdentity target)
    {
        super(thisPeer, id);
    }
    
    @Override
    public void initiate()
    {
        Message msg = createMessage(Performative.Request, this);
        combine(msg, 
                struct(CONTENT, request)); 
        send(target, msg);        
    }

    @FromState("Started")
    @OnMessage(performative="Request")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onRequest(Message msg) throws Throwable
    {
        Request request = getPart(msg, CONTENT);
        Response response = doRequest(request);
//        Performative.
        Message reply = getReply(msg, Performative.Agree);
        send(getSender(msg), reply);
        return WorkflowState.Completed;
    }
       
    public Request getRequest()
    {
        return request;
    }

    public void setRequest(Request request)
    {
        this.request = request;
    }

    public Response getResponse()
    {
        return response;
    }

    public void setResponse(Response response)
    {
        this.response = response;
    }
    
    public HGPeerIdentity getTarget()
    {
        return target;
    }
    
    public void setTarget(HGPeerIdentity target)
    {
        this.target = target;
    }     
}