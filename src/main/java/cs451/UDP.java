package cs451;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static cs451.Main.addrToId;
import static cs451.Main.idToHost;
import static cs451.Packet.PKT_SIZE;

public class UDP {

    private final DatagramSocket socket;
    public final static byte[] ACK = "A".getBytes();
    private Map<Integer, PerfectLink> idToPerfectLinks;
    private final ConcurrentLinkedQueue<DatagramPacket> dps = new ConcurrentLinkedQueue<>();


    public UDP(Host sender) {
        socket = sender.getSocket();
        new Thread(this::listen).start();
        new Thread(this::handleReceivedPackets).start();
        System.out.println("UDP listening thread started.");
    }

    public void setIdToPerfectLinks(Map<Integer, PerfectLink> idToPerfectLinks) {
        this.idToPerfectLinks = idToPerfectLinks;
    }

    public void send(Packet pkt, Host receiver) {
        byte[] content = pkt.getContent();
        DatagramPacket dp = new DatagramPacket(content, content.length, receiver.getInetAddress(), receiver.getPort());
        try {
            socket.send(dp);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.out.println("UDP send "+pkt.getInitialSenderId()+" "+pkt.getSeqNum()); // I send several times the same pkt: why? (way more than twice)
    }

    public void listen() {
        while (true) {

            System.out.println("UDP listening.");
            DatagramPacket dp = new DatagramPacket(new byte[PKT_SIZE], PKT_SIZE);
            try {
                System.out.println("Waiting for msgs in UDP socket.");
                socket.receive(dp);
                System.out.println("After receive in UDP socket.");
            } catch (IOException e) {
                e.printStackTrace();
            }
            dps.add(dp);
        }
    }

    private void handleReceivedPackets() {
        while (true) {
            if (!dps.isEmpty()) {
                dps.forEach(dp -> {
                    byte[] payload = dp.getData(); //TODO: maybe put rcved payloads in a queue for some other thread to deal with
                    InetAddress senderInetAddress = dp.getAddress();
                    int senderPort = dp.getPort();
                    String completeSenderAddr = senderInetAddress.toString().substring(1).concat(":".concat(String.valueOf(senderPort)));
                    int senderId = addrToId.get(completeSenderAddr);

                    DataInputStream ds = new DataInputStream(new ByteArrayInputStream(payload));
                    int seqNum = 0;
                    try {
                        seqNum = ds.readInt();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    int initialSenderId = -1;
                    try {
                        initialSenderId = ds.readInt();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    PerfectLink perfectLink = idToPerfectLinks.get(senderId); // get PL corresponding to sender and deliver on this PL

                    // check if pkt is an ACK
                    if (isAck(ds)) {
                        System.out.println("ACK "+senderId+" "+seqNum);
                        perfectLink.handleAck(seqNum);

                    } else {
                        System.out.println("UDP receive "+senderId+" "+seqNum);
                        Packet pkt = new Packet(seqNum, initialSenderId);
                        Map<Integer, Integer> lsnFromAffectingProc = new ConcurrentHashMap<>();
                        if (hasVectorClock(ds,lsnFromAffectingProc)) {
                            pkt.addVectorClock(lsnFromAffectingProc);
                        }
                        perfectLink.deliver(idToHost.get(senderId), pkt);
                    }
                });
            }
        }
    }

    private boolean isAck(DataInputStream ds) {
        byte[] content = new byte[ACK.length];
        try {
            ds.readNBytes(content, 0, ACK.length);
        } catch (Exception ignore) {}
        return Arrays.equals(content, ACK);
    }

    private boolean hasVectorClock(DataInputStream ds, Map<Integer, Integer> lsnFromAffectingProc) {
        boolean isLsn = false;
        int hostId = 0;
        int n_int_read = 0;
        int readInt = -1;
        while (n_int_read==0 || readInt!=-1) {
            try {
                readInt = ds.readInt();
            } catch (Exception ignore) {}
            if (readInt!=-1) {
                n_int_read +=1;
                // read hostId then lsn alternatively, when have both put in map
                if (isLsn) {
                    lsnFromAffectingProc.put(hostId, readInt);
                    isLsn = false;
                } else {
                    isLsn = true;
                    hostId = readInt;
                }
            }
        }
        return n_int_read>0;
    }


}
