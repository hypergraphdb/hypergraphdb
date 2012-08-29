package org.hypergraphdb.peer.workflow;


import static org.hypergraphdb.peer.Messages.CONTENT;
import static org.hypergraphdb.peer.Messages.getReply;
import static org.hypergraphdb.peer.Messages.getSender;
import java.util.UUID;
import mjson.Json;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.Performative;

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
    	Json msg = createMessage(Performative.Request, this);
        msg.set(CONTENT, request); 
        send(target, msg);        
    }

	@FromState("Started")
    @OnMessage(performative="Request")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onRequest(Json msg) throws Throwable
    {
        Request request = Messages.fromJson(msg.at(CONTENT));
        Response response = doRequest(request);
//        Performative.
        Json reply = getReply(msg, Performative.Agree);
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