package cs451;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Packet {

    private int seqNum;
    private final int initialSenderId;
    private byte[] data;
    public static int PKT_SIZE = 8;
    private int dataLen = 0;
    private Map<Integer, Integer> lsnFromAffectingProc;

    public Packet(int seqNum, int initialSenderId) {
        this.seqNum = seqNum;
        this.initialSenderId = initialSenderId;
    }

    public Packet(int seqNum, int initialSenderId, byte[] data) {
        this(seqNum, initialSenderId);
        if (data != null) {
            dataLen = data.length;
            this.data = Arrays.copyOf(data, dataLen);
        }
        PKT_SIZE += dataLen;
    }
    
    public void addVectorClock(Map<Integer, Integer> lsnFromAffectingProc) {
        this.lsnFromAffectingProc = new ConcurrentHashMap<>(lsnFromAffectingProc);
        ByteArrayOutputStream bs = new ByteArrayOutputStream(4*2*lsnFromAffectingProc.size()); // nb bytes = 4 * nb int
        // create 'list' representing map(hostId->lsn) as [hostId1, lsn1, hostId2, lsn2, ...]
        DataOutputStream ds = new DataOutputStream(bs);
        for (Map.Entry<Integer, Integer> entry : lsnFromAffectingProc.entrySet()) {
            try {
                ds.writeInt(entry.getKey());
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                ds.writeInt(entry.getValue());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        data = bs.toByteArray();
        PKT_SIZE += data.length;
    }

    @Override
    public boolean equals(Object obj) {
        if (this==obj) {
            return true;
        }
        if (!(obj instanceof Packet)) {
            return false;
        }
        Packet pkt = (Packet) obj;
        return pkt.getSeqNum() == getSeqNum()
                && pkt.getInitialSenderId() == getInitialSenderId();
    }

    public byte[] getContent() {
        int seqAndIdOffset = 8;
        ByteArrayOutputStream bs = new ByteArrayOutputStream(seqAndIdOffset + dataLen);
        DataOutputStream ds = new DataOutputStream(bs);
        try {
            ds.writeInt(seqNum);
            ds.writeInt(initialSenderId);
            if (data != null) {
                ds.write(data);
            }
        } catch (Exception ignore) {
        }
        return bs.toByteArray();
    }

    public int getSeqNum() {
        return seqNum;
    }

    public void setSeqNum(int seqNum) {
        this.seqNum = seqNum;
    }

    public int getInitialSenderId() {
        return initialSenderId;
    }

    public int hashcode() {
        int hash = 7;
        hash = 31 * hash + seqNum;
        hash = 31 * hash + initialSenderId;
        return hash;
    }

    public boolean smallerVectorClockThan(Map<Integer, Integer> greaterVC) {
        if (lsnFromAffectingProc == null) {
            return false;
        }
        for (Map.Entry<Integer, Integer> entry : lsnFromAffectingProc.entrySet()) {
            if (greaterVC.getOrDefault(entry.getKey(),0) < entry.getValue()) {
                return false;
            }
        }
        return true;
    }
}
