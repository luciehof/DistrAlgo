package cs451;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static cs451.Main.outputQueue;

public class FIFO {
    private final AtomicInteger lsn = new AtomicInteger();
    private final Set<Packet> pending;
    private final Map<Integer, Integer> next;
    private final URB urb;

    public FIFO(URB urb, int nbHosts) {
        this.urb = urb;
        lsn.set(0);
        pending = ConcurrentHashMap.newKeySet();
        next = new ConcurrentHashMap<>();
        for (int i = 1; i <= nbHosts; ++i) {
            next.put(i, 1);
        }
    }

    public void fifoBroadcast(Packet pkt) {
        String event = "b " + pkt.getSeqNum();
        outputQueue.add(event);
        lsn.addAndGet(1);
        pkt.setSeqNum(lsn.get());
        urb.urbBroadcast(pkt);
    }

    public void urbDeliver(Packet pkt) {
        pending.add(pkt);
        synchronized (outputQueue) {
            for (Packet p : pending) {
                int nextSn = next.get(p.getInitialSenderId());
                if (p.getSeqNum() == nextSn) {
                    pending.remove(p);
                    fifoDeliver(p);
                    next.replace(p.getInitialSenderId(), nextSn + 1);
                }
            }
       }
    }

    public void fifoDeliver(Packet pkt) {
        String event = "d " + pkt.getInitialSenderId() + " " + pkt.getSeqNum();
        outputQueue.add(event);
    }
}
