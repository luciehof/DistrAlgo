package cs451;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import static cs451.UDP.ACK;


public class PerfectLink {

    private final Host self;
    private final Host otherProcess;
    private final PriorityBlockingQueue<Integer> noAck;
    private static final int TIMEOUT = 2000; //TODO: (timeout should be greater than 2*(trans_time + max delay) )
    private final Map<Integer, Packet> snToPkt;
    private final Set<Packet> delivered;
    private URB urb;
    private final UDP udp;

    public PerfectLink(Host self, Host otherProcess, UDP udp) {
        this.self = self;
        noAck = new PriorityBlockingQueue<>();
        snToPkt = new ConcurrentHashMap<>();
        delivered = ConcurrentHashMap.newKeySet();
        this.udp = udp;
        this.otherProcess = otherProcess;
    }

    public void send(Packet pkt) {
        // if didnt reach wd bound then send else wait for ack
        if (!snToPkt.containsKey(pkt.getSeqNum())) {
            snToPkt.put(pkt.getSeqNum(), pkt);
            noAck.add(pkt.getSeqNum());
        }
        udp.send(pkt, otherProcess);
        //waitForAck(); // TODO: remove comment!!
    }

    public void deliver(Host sender, Packet pkt) {
        System.out.println("PL deliver "+pkt.getInitialSenderId()+" "+pkt.getSeqNum());
        if (!delivered.contains(pkt)) {
            urb.bebDeliver(sender, pkt);
            delivered.add(pkt);
        }

        Packet ackPkt = new Packet(pkt.getSeqNum(), self.getId(), ACK);
        udp.send(ackPkt, otherProcess);
    }

    private void waitForAck() {
        try {
            Thread.sleep(TIMEOUT);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (!noAck.isEmpty()) {
            noAck.forEach(seqNum -> send(snToPkt.get(seqNum)));
        }
    }

    public void handleAck(int seqNum) {
        noAck.remove(seqNum);
        // here receive new ack: if wd bound was reached, can now send a new pkt (boolean to allow sending?)
    }

    public void setUrb(URB urb) {
        this.urb = urb;
    }
}
