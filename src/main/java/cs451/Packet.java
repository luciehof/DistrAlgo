package cs451;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;

public class Packet {

    private int seqNum;
    private final int initialSenderId;
    private byte[] data;
    public static int PKT_SIZE = 10;
    private int dataLen = 0;

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
    }
    
    public void addVectorClock(List<Integer> vc) {
        ByteArrayOutputStream bs = new ByteArrayOutputStream(vc.size());
        DataOutputStream ds = new DataOutputStream(bs);
        //vc.forEach(ds::writeInt);
        data = bs.toByteArray();
    }

    //public getVC ?

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
}
