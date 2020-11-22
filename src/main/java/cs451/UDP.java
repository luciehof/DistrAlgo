package cs451;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;

import static cs451.Main.addrToId;
import static cs451.Main.idToHost;
import static cs451.Packet.PKT_SIZE;

public class UDP {

    private final DatagramSocket socket;
    public final static byte[] ACK = "A".getBytes();
    private Map<Integer, PerfectLink> idToPerfectLinks;


    public UDP(Host sender) {
        socket = sender.getSocket();
        new Thread(this::listen).start();
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
    }

    public void listen() {
        while (true) {

            DatagramPacket dp = new DatagramPacket(new byte[PKT_SIZE], PKT_SIZE);
            try {
                socket.receive(dp);
            } catch (IOException e) {
                e.printStackTrace();
            }

            byte[] payload = dp.getData();
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

            // read data in ds, if -1, no more data, else check if ACK
            byte[] content = new byte[ACK.length];
            // check if pkt is an ACK
            if (hasMoreContent(ds, content) && Arrays.equals(content, ACK)) {
                perfectLink.handleAck(seqNum);

            } else {
                perfectLink.deliver(idToHost.get(senderId), new Packet(seqNum, initialSenderId));
            }
        }
    }

    private boolean hasMoreContent(DataInputStream ds, byte[] content) {
        int read_bytes = -1;
        try {
            read_bytes = ds.read(content, 0, ACK.length);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return read_bytes != -1;
    }


}
