package cs451;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


public class Main {
    private static boolean CRASHED = false;
    public static Map<String, Integer> addrToId; // This map is used only to get the senderId when process receives a msg
    public static Map<Integer, Host> idToHost; // This map is used to get the host corresponding to the senderId
    public static final ConcurrentLinkedQueue<String> outputQueue = new ConcurrentLinkedQueue<>();
    private static String outputPath;

    private static void handleSignal() {
        //immediately stop network packet processing
        CRASHED = true;
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
        System.out.println("Writing output.");
        writeOutput();
    }

    private static void writeOutput() {
        try {
            BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputPath, false));
            outputQueue.forEach(event -> {
                try {
                    outputWriter.write(event);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    outputWriter.newLine();
                } catch (IOException e) {
                    System.out.println("An error occurred when writing event.");
                    e.printStackTrace();
                }
            });
            outputWriter.close();
            System.out.println("Successfully wrote to output file.");
        } catch (IOException e) {
            System.out.println("An error occurred in output writer.");
            e.printStackTrace();
        }
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID is " + pid + ".");
        System.out.println("Use 'kill -SIGINT " + pid + " ' or 'kill -SIGTERM " + pid + " ' to stop processing packets.");

        System.out.println("My id is " + parser.myId() + ".");
        System.out.println("List of hosts is:");
        for (Host host : parser.hosts()) {
            System.out.println(host.getId() + ", " + host.getIp() + ", " + host.getPort());
        }

        System.out.println("Barrier: " + parser.barrierIp() + ":" + parser.barrierPort());
        System.out.println("Signal: " + parser.signalIp() + ":" + parser.signalPort());
        System.out.println("Output: " + parser.output());
        outputPath = parser.output();
        // if config is defined; always check before parser.config()
        if (parser.hasConfig()) {
            System.out.println("Config: " + parser.config());
        }

        // hawhmap of hostIP_port=>hostPID
        addrToId = new ConcurrentHashMap<>();
        idToHost = new ConcurrentHashMap<>();

        for (Host host : parser.hosts()) {
            addrToId.put(host.getIp().concat(":".concat(String.valueOf(host.getPort()))), host.getId());
            idToHost.put(host.getId(), host);
        }

        Host currentProcess = idToHost.get(parser.myId());

        // get nb of msgs to broadcast per process
        int NB_MSGS = getNbMsg(parser);

        UDP udp = new UDP(currentProcess);
        Map<Integer, PerfectLink> idToPerfectLinks = new ConcurrentHashMap<>();
        for (Host host : parser.hosts()) {
            PerfectLink perfectLink = new PerfectLink(currentProcess, host, udp);
            idToPerfectLinks.put(host.getId(), perfectLink);
        }
        udp.setIdToPerfectLinks(idToPerfectLinks); // can now start receiving msgs from those links and deliver them to appropriate PL
        URB urb = new URB(currentProcess, idToPerfectLinks); // need to set urb in every PL
        //FIFO fifo = new FIFO(urb, parser.hosts().size());
        //urb.setFifo(fifo);
        LCausal lCausal = new LCausal(urb, parser.hosts().size(), currentProcess.getId(), getAffectingProc(parser));
        urb.setLCausal(lCausal);

        Coordinator coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());

        System.out.println("Waiting for all processes for finish initialization");
        coordinator.waitOnBarrier();

        System.out.println("Broadcasting messages...");
        for (int seqNum = 1; seqNum <= NB_MSGS; ++seqNum) {
            if (CRASHED) {
                break;
            }
            Packet packet = new Packet(seqNum, currentProcess.getId());
            //fifo.fifoBroadcast(packet);
            lCausal.lCausalBroadcast(packet);
        }

        System.out.println("Signaling end of broadcasting messages");
        coordinator.finishedBroadcasting();

        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }

    private static int getNbMsg(Parser parser) {
        Scanner scanner = null;
        try {
            scanner = new Scanner(new File(parser.config()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (scanner != null) {
            return scanner.nextInt();
        } else {
            return 1;
        }
    }

    private static List<Integer> getAffectingProc(Parser parser) {
        List<Integer> affectingProc = new ArrayList<>();
        Scanner scanner = null;
        try {
            scanner = new Scanner(new File(parser.config()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (scanner != null) {
            assert scanner.hasNextLine();
            scanner.nextLine();

            int affectedProc = scanner.nextInt();
            while (affectedProc != parser.myId() && scanner.hasNextLine()) {
                scanner.nextLine();
                assert scanner.hasNextInt();
                affectedProc = scanner.nextInt();
            }

            String myDependencies = scanner.nextLine();
            scanner.close();
            scanner = new Scanner(myDependencies);
            while (scanner.hasNextInt()) {
                affectingProc.add(scanner.nextInt());
            }
            return affectingProc;
        } else {
            return affectingProc;
        }
    }
}
