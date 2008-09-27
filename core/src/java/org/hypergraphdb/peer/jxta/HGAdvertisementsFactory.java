package org.hypergraphdb.peer.jxta;

import net.jxta.document.AdvertisementFactory;
import net.jxta.peer.PeerID;
import net.jxta.pipe.PipeID;
import net.jxta.pipe.PipeService;
import net.jxta.protocol.PeerAdvertisement;
import net.jxta.protocol.PipeAdvertisement;

public class HGAdvertisementsFactory {
   
    public static PipeAdvertisement newPipeAdvertisement(PipeID pipeId, String pipeName) {
       
        PipeAdvertisement advertisement = (PipeAdvertisement)
                AdvertisementFactory.newAdvertisement(PipeAdvertisement.getAdvertisementType());

        advertisement.setPipeID(pipeId);
        advertisement.setType(PipeService.UnicastType);
        advertisement.setName(pipeName);
        
        return advertisement;
    }
    
    
    public static PeerAdvertisement newPeerAdvertisement(PeerID peerId) {

    	PeerAdvertisement adv = (PeerAdvertisement) AdvertisementFactory.newAdvertisement(PeerAdvertisement.getAdvertisementType());
    	adv.setPeerID(peerId);
    	adv.setName("HGDB Peer");
        return adv;
    }
}
 