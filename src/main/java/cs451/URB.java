package cs451;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class URB {

    private static int MAJORITY;
    private final Set<Packet> forward;
    private final Set<Packet> delivered;
    private final Map<Integer, Set<Integer>> ack; // pkt hash to processes id
    private final Host self;
    Set<PerfectLink> perfectLinks;
    private FIFO fifo;
    private LCausal lCausal;

    public URB(Host self, Map<Integer, PerfectLink> idToPerfectLinks) {
        this.self = self;
        this.perfectLinks = ConcurrentHashMap.newKeySet();
        perfectLinks.addAll(idToPerfectLinks.values());
        MAJORITY = perfectLinks.size() / 2;
        initPerfectLinks();
        forward = ConcurrentHashMap.newKeySet();
        delivered = ConcurrentHashMap.newKeySet();
        ack = new ConcurrentHashMap<>();
    }

    private void initPerfectLinks() {
        for (PerfectLink perfectLink : perfectLinks) {
            perfectLink.setUrb(this);
        }
    }

    public void urbBroadcast(Packet pkt) {
        addToAck(self, pkt);
        bebBroadcast(pkt);
        forward.add(pkt);
        deliverForward();
    }

    public void urbDeliver(Packet pkt) {
        lCausal.urbDeliver(pkt);
        //fifo.urbDeliver(pkt);
    }

    // bebDeliver is called by lower layer PerfectLink's deliver function
    public void bebDeliver(Host sender, Packet pkt) {
        addToAck(sender, pkt);

        if (!forward.contains(pkt)) {
            bebBroadcast(pkt);
            forward.add(pkt);
        }
        deliverForward();
    }

    private void bebBroadcast(Packet pkt) {
        new Thread(() -> {
            for (PerfectLink perfectLink : perfectLinks) {
                perfectLink.send(pkt);
            }
        }).start();
    }

    private void deliverForward() {
        for (Packet pkt : forward) {
            if (ack.getOrDefault(pkt.hashcode(), new HashSet<>()).size() >= MAJORITY && !delivered.contains(pkt)) {
                delivered.add(pkt);
                urbDeliver(pkt);
            }
        }
    }

    private void addToAck(Host sender, Packet pkt) {
        if (!ack.containsKey(pkt.hashcode())) {
            Set<Integer> ids = new HashSet<>();
            ids.add(sender.getId());
            ack.put(pkt.hashcode(), ids);
        } else {
            ack.get(pkt.hashcode()).add(sender.getId());
        }
    }

    public void setFifo(FIFO fifo) {
        this.fifo = fifo;
    }

    public void setLCausal(LCausal lCausal) {
        this.lCausal = lCausal;
    }
}
