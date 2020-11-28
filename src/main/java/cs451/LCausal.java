package cs451;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static cs451.Main.outputQueue;

public class LCausal {

    private final AtomicInteger lsn = new AtomicInteger();
    private final Set<Packet> pending;
    private final URB urb;
    private final Map<Integer, Integer> lsnFromAffectingProc;
    private final Map<Integer, Integer> lsnFromAllProc;
    private final int myId;

    public LCausal(URB urb, int nProc, int currentId, List<Integer> affectingProc) {
        myId = currentId;
        this.urb = urb;
        pending = ConcurrentHashMap.newKeySet();
        lsn.set(0);
        lsnFromAffectingProc = new ConcurrentHashMap<>(); // init with all affecting processes as keys
        for (int hostId : affectingProc) {
            lsnFromAffectingProc.put(hostId,0);
        }
        lsnFromAllProc = new ConcurrentHashMap<>(); // init with all processes as keys
        for (int hostId=0; hostId<nProc; ++hostId) {
            lsnFromAllProc.put(hostId, 0);
        }
    }

    public void lCausalBroadcast(Packet pkt) {
        String event = "b " + pkt.getSeqNum();
        outputQueue.add(event);
        lsnFromAffectingProc.put(myId, lsn.get());
        lsnFromAllProc.put(myId, lsn.get());
        lsn.addAndGet(1);
        pkt.setSeqNum(lsn.get());
        pkt.addVectorClock(lsnFromAffectingProc);
        urb.urbBroadcast(pkt);
    }

    public void lCausalDeliver(Packet pkt) {
        String event = "d " + pkt.getInitialSenderId() + " " + pkt.getSeqNum();
        outputQueue.add(event);
    }

    public void urbDeliver(Packet pkt) {
        pending.add(pkt);
        for (Packet p : pending) {
            if (p.smallerVectorClockThan(lsnFromAllProc)) {
                pending.remove(p);
                int prev = lsnFromAllProc.get(p.getInitialSenderId());
                if (lsnFromAffectingProc.containsKey(p.getInitialSenderId())) {
                    lsnFromAffectingProc.replace(p.getInitialSenderId(), prev+1);
                }
                lsnFromAllProc.replace(p.getInitialSenderId(), prev+1);
                lCausalDeliver(p);
            }
        }
    }

}
