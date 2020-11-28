package cs451;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static cs451.Main.outputQueue;

public class LCausal {

    private final AtomicInteger lsn = new AtomicInteger();
    private final Set<Packet> pending;
    private final URB urb;
    private final CopyOnWriteArrayList<Integer> vectorClock;

    public LCausal(URB urb, int nProc) {
        this.urb = urb;
        pending = ConcurrentHashMap.newKeySet();
        lsn.set(0);
        vectorClock = new CopyOnWriteArrayList(Collections.singleton(new int[nProc]));
    }

    public void lCausalBroadcast(Packet pkt) {
        String event = "b " + pkt.getSeqNum();
        outputQueue.add(event);
        //vectorClock.add(myId, lsn);
        lsn.addAndGet(1);
        //urb.urbBroadcast(vectorClock, pkt); // TODO: construct packet with previous pkt value and new VC
    }

    public void lCausalDeliver(Packet pkt) {
        String event = "d " + pkt.getInitialSenderId() + " " + pkt.getSeqNum();
        outputQueue.add(event);
    }

    public void urbDeliver(Packet pkt) {
        pending.add(pkt);
        for (Packet p : pending) {
            //if (p.getVectorClock() <= vectorClock) { // TODO: function to compare every values of VCs
                pending.remove(p);
                int rank = p.getInitialSenderId()-1;
                int prev = vectorClock.get(rank);
                vectorClock.add(rank, prev+1);
                lCausalDeliver(p);
            //}
        }
    }

}
