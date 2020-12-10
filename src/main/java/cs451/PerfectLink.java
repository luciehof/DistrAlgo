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

    private final Set<Packet> pktsToSend;

    public PerfectLink(Host self, Host otherProcess, UDP udp) {
        this.self = self;
        noAck = new PriorityBlockingQueue<>();
        snToPkt = new ConcurrentHashMap<>();
        delivered = ConcurrentHashMap.newKeySet();
        this.udp = udp;
        this.otherProcess = otherProcess;

        pktsToSend = ConcurrentHashMap.newKeySet();
        new Thread(this::sendingThread).start();
    }

    private void sendingThread() {
        while (true) {
            if (!pktsToSend.isEmpty()) {
                for (Packet pkt : pktsToSend) {
                    this.send(pkt);
                    pktsToSend.remove(pkt);
                }
            }
        }
    }

    public void addPktToSend(Packet pkt) {
        pktsToSend.add(pkt);
    }

    public void send(Packet pkt) {
        if (!snToPkt.containsKey(pkt.getSeqNum())) {
            snToPkt.put(pkt.getSeqNum(), pkt);
            noAck.add(pkt.getSeqNum());
        }
        udp.send(pkt, otherProcess);
        waitForAck();
    }

    public void deliver(Host sender, Packet pkt) {
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
    }

    public void setUrb(URB urb) {
        this.urb = urb;
    }
}
