import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Controller {
    private static final Logger logger = Logger.getLogger(Controller.class.getName());

    private final int cport; //controller port
    private final int repFactor;
    private final int timeout;  //milliseconds
    private final int rebalancePeriod;  //milliseconds
    private boolean canRebalance;
    private final AtomicBoolean storing;
    private final AtomicBoolean removing;
    private final AtomicBoolean rebalancing;
    private final List<Integer> dstorePorts;
    private final Map<Integer,Socket> dstorePortsSockets;
    private final Map<String, List<Integer>> filesStored; //maps filename to dstorePort
    private final Map<String, Integer> fileSizes;
    private final Map<String, Integer> fileAckCount;
    private final Map<String, Boolean> fileStoreComplete;
    private final List<String> fileRemoveComplete;
    private final Map<String, Socket> fileSocket;
    private final Map<Socket, List<Integer>> canReload;
    private final Map<String, List<Integer>> filesInLoad;

    public Controller(int cport, int repFactor, int timeout, int rebalancePeriod){
        this.cport = cport;
        this.repFactor = repFactor;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;

        this.storing = new AtomicBoolean(false);
        this.removing = new AtomicBoolean(false);
        this.rebalancing = new AtomicBoolean(false);

        this.dstorePorts = Collections.synchronizedList(new ArrayList<>());
        this.dstorePortsSockets = Collections.synchronizedMap(new HashMap<>());
        this.filesStored = Collections.synchronizedMap(new HashMap<>());
        this.fileSizes = Collections.synchronizedMap(new HashMap<>());
        this.fileAckCount = Collections.synchronizedMap(new HashMap<>());
        this.fileStoreComplete = Collections.synchronizedMap(new HashMap<>());
        this.fileRemoveComplete = Collections.synchronizedList(new ArrayList<>());
        this.fileSocket = Collections.synchronizedMap(new HashMap<>());
        this.canReload = Collections.synchronizedMap(new HashMap<>());
        this.filesInLoad = Collections.synchronizedMap(new HashMap<>());

        canRebalance = true;
    }


    public static void main(String[] args){
        if (args.length != 4){
            System.out.println("Invalid arguments");
            return;
        }

        int cport = Integer.parseInt(args[0]);
        int repFactor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);

        Controller controller = new Controller(cport,repFactor,timeout,rebalancePeriod);
        controller.start();
    }

    public void start(){
        new Thread(this::clientRequests).start();
        //new Thread(this::waitForRebalance).start();
    }

    private void clientRequests() {
        try (ServerSocket serverSocket = new ServerSocket(cport)){
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(()->handleClientRequests(socket)).start();
            }
        }
        catch (IOException e){

        }
    }

    private void handleClientRequests(Socket socket){
        while (true){
            String input = null;
            PrintWriter writer;
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                writer = new PrintWriter(socket.getOutputStream());
                input = reader.readLine();

                if (input == null) { //only when connection closed
                    throw new IOException();
                }
            }catch(IOException e){
                break;
            }
            String finalInput = input;
            if (input.startsWith(Protocol.JOIN_TOKEN)) {
                logger.info("Join received");
                new Thread(()->handleJOIN(socket, finalInput)).start();
            }
            else if (input.startsWith(Protocol.STORE_ACK_TOKEN)) {
                logger.info("Store ACKNOWLEDGEMENT received");
                new Thread(()->handleSTORE_ACK(finalInput)).start();
            } else if (input.startsWith(Protocol.STORE_TOKEN)) {
                storing.set(true);
                logger.info("Store received");
                new Thread(()->handleSTORE(socket, finalInput, writer)).start();
            }
            else if (input.startsWith(Protocol.LOAD_TOKEN)) {
                logger.info("Load received");
                new Thread(()->handleLOAD(socket, finalInput, writer)).start();
            }
            else if (input.startsWith(Protocol.REMOVE_ACK_TOKEN)){
                logger.info("Remove ACKNOWLEDGEMENT received, I'm ignoring this");
                //new Thread(()->handleREMOVE_ACK(finalInput)).start();
            }
            else if (input.startsWith(Protocol.REMOVE_TOKEN)) {
                removing.set(true);
                logger.info("Remove received");
                new Thread(()->handleREMOVE(socket, finalInput, writer)).start();
            }
            else if (input.startsWith(Protocol.LIST_TOKEN)) {
                logger.info("List received");
                new Thread(()->handleLIST(socket, writer)).start();
            } else if (input.startsWith(Protocol.RELOAD_TOKEN)) {
                logger.info("Reload received");
                new Thread(()->handleRELOAD(socket, finalInput, writer)).start();
            }
        }

    }

    private void handleDstoreRequests(Socket socket){
        try{
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            while (true){
                String input =null;

                try{
                    input = reader.readLine();
                    if(input == null){
                        logger.info("THERES NO INPUT ");
                        throw new IOException();
                    }
                    logger.info("HEY THIS IS THE INPUT "+input);

                }catch (IOException e){
                    logger.info("Handling Dstore failure");
                    handleDstoreFailure(socket);
                }
                if (input != null){
                    if (input.startsWith(Protocol.STORE_ACK_TOKEN)){
                        logger.info("Store ack received");
                        String finalInput = input;
                        if (storing.get()) {
                            new Thread(() -> handleSTORE_ACK(finalInput)).start();
                        }
                    } else if (input.startsWith(Protocol.REMOVE_ACK_TOKEN)){
                        logger.info("Remove ack received");
                        String finalInput1 = input;
                        if (removing.get()){
                            new Thread(()->handleREMOVE_ACK(finalInput1)).start();
                        }
                    } else if (input.startsWith(Protocol.LIST_TOKEN)){
                        //new Thread(()->handleDstoreLIST(socket)).start();
                    } else if (input.startsWith(Protocol.REBALANCE_COMPLETE_TOKEN)){
                        //new Thread(()->handleREBALANCE_COMPLETE()).start();
                    } else if (input.startsWith(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN)){
                        String finalInput2 = input;
                        //new Thread(()->handleERROR_FILE_DOES_NOT_EXIST(finalInput2)).start();
                    } else {
                        logger.info(input + "Uhhh, boss, this was sent.");
                    }
                }

            }
        } catch (Exception e){
            logger.info("Well something must've gone wrong: "+e);
        }
    }

    private synchronized void handleDstoreFailure(Socket socket) {
        Integer failed = null;
        for (Integer sock : dstorePortsSockets.keySet()) {
            if (dstorePortsSockets.get(sock) == socket) {
                failed = sock;
                break;
            }
        }

        if (failed != null) {
            dstorePorts.remove(failed);
            dstorePortsSockets.remove(failed);

            for (String file : filesStored.keySet()) {
                List<Integer> ports = filesStored.get(file);
                ports.remove(failed);

                if (ports.size() < repFactor) {
                    Boolean completed = fileStoreComplete.getOrDefault(file, false);
                    if (!completed) {
                        filesStored.remove(file);
                        fileSizes.remove(file);
                        fileStoreComplete.remove(file);
                        fileAckCount.remove(file);
                        fileSocket.remove(file);
                        logger.info("Removed incomplete file after Dstore crash: " + file);
                    }
                }
                if (ports.isEmpty()) {
                    filesStored.remove(file);
                    fileSizes.remove(file);
                    fileStoreComplete.remove(file);
                    logger.info("Cleaned up empty stores");
                }
            }
            logger.info("Handled dstore failure");
        }

    }

    private synchronized void handleJOIN(Socket socket, String input){
        if (input.split(" ").length == 2){
            int dstorePort = Integer.parseInt(input.split(" ")[1]);

            if(!dstorePorts.contains(dstorePort)){
                logger.info("Joined port");
                dstorePorts.add(dstorePort);
                dstorePortsSockets.put(dstorePort,socket);
                new Thread (()-> handleDstoreRequests(socket)).start();

//                if (dstorePorts.size() > repFactor){
//                    /**
//                     * DO STUFF HERE
//                     * Rebalance or something
//                     */
//                }
            }
        }
    }

    private synchronized void handleSTORE(Socket socket, String input, PrintWriter writer){
        if (input.split(" ").length == 3){
            String filename = input.split(" ")[1];
            int filesize = Integer.parseInt(input.split(" ")[2]);

            try{
                if (dstorePorts.size() < repFactor){
                    writer.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    writer.flush();
                    storing.set(false);
                    return;
                }

                if (filesStored.containsKey(filename)){
                    writer.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    writer.flush();
                    return;
                }

                List<Integer> selectedDstores = dstorePorts.stream().sorted(Comparator.comparingInt(this::countFilesForDstore)).limit(repFactor).toList();
                filesStored.put(filename, selectedDstores);
                fileAckCount.put(filename, 0);
                fileStoreComplete.put(filename, false);
                fileSocket.put(filename, socket);
                fileSizes.put(filename,filesize);

                String storeTo = selectedDstores.stream().map(Object::toString).collect(Collectors.joining(" "));
                writer.println(Protocol.STORE_TO_TOKEN + " " +storeTo);
                writer.flush();
                logger.info("Sent STORE_TO "+storeTo);

                ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
                scheduler.schedule(()->{
                    if (fileAckCount.containsKey(filename) && storing.get()) {

                        if (fileAckCount.get(filename) < repFactor) { // Needs cleanup due to missing acks
                            filesStored.remove(filename);
                            fileAckCount.remove(filename);
                            fileStoreComplete.remove(filename);
                            fileSocket.remove(filename);
                            fileSizes.remove(filename);
                        } else {
                            fileStoreComplete.put(filename, true);
                        }

                        logger.info("Timed out store");
                    }
                },timeout, TimeUnit.MILLISECONDS);
            } catch (Exception e){
                storing.set(false);
            }
        }

    }

    private int countFilesForDstore(int port) {
        int count = 0;
        synchronized (filesStored) {
            for (List<Integer> ports : filesStored.values()) {
                if (ports.contains(port)) {
                    count++;
                }
            }
        }
        return count;
    }

    private synchronized void handleSTORE_ACK(String input) {
        if (input.split(" ").length == 2){
            String filename = input.split(" ")[1];

            logger.info("Handling STORE_ACK" + fileAckCount.get(filename));
            fileAckCount.put(filename, fileAckCount.get(filename)+1);
            if (fileAckCount.get(filename) >= repFactor) {
                fileStoreComplete.put(filename,true);
                storing.set(false);
                try{
                    logger.info("Sending STORE_COMPLETE");
                    PrintWriter writer = new PrintWriter(fileSocket.get(filename).getOutputStream());
                    writer.println(Protocol.STORE_COMPLETE_TOKEN);
                    writer.flush();
                    fileAckCount.put(filename,0);
                } catch (Exception ignored){

                }
            }
        }

    }

    private synchronized void handleLOAD(Socket socket, String input, PrintWriter writer){
        if (input.split(" ").length == 2){
            String filename = input.split(" ")[1];
            if (!filesInLoad.containsKey(filename)){
                filesInLoad.put(filename, new ArrayList<>());
            }

            if (dstorePorts.size() < repFactor){
                writer.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                writer.flush();
                return;
            }

            if (!filesStored.containsKey(filename)){
                writer.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                writer.flush();
                return;
            }
            List<Integer> ports = filesStored.get(filename).stream().filter(dstorePorts::contains).toList();

            if (ports.isEmpty()) {
                writer.println(Protocol.ERROR_LOAD_TOKEN);
                writer.flush();
                return;
            }

            canReload.put(socket, new ArrayList<>(ports));
            int port = ports.get(0);

            int fileSize = fileSizes.get(filename);
            writer.println(Protocol.LOAD_FROM_TOKEN + " " + port + " " + fileSize);
            writer.flush();
            logger.info("Sending LOAD_FROM");
        }

    }

    private synchronized void handleRELOAD(Socket socket, String input, PrintWriter writer) {
        if (input.split(" ").length == 2) {
            String filename = input.split(" ")[1];

            logger.info("Handling RELOAD");
            if (canReload.containsKey(socket)) {
                List<Integer> toReload = canReload.get(socket);
                toReload.removeIf(port -> !dstorePorts.contains(port));

                if (!toReload.isEmpty()){
                    int port = toReload.removeLast();
                    int fileSize = fileSizes.get(filename);
                    writer.println(Protocol.LOAD_FROM_TOKEN+" "+port+" "+fileSize);
                    writer.flush();
                } else{
                    canReload.remove(socket);
                    writer.println(Protocol.ERROR_LOAD_TOKEN);
                    writer.flush();
                    logger.info("Sending ERROR_LOAD");
                }
            } else {
                logger.info("Need to load before reload");
            }
        }
    }

    private synchronized void handleREMOVE(Socket socket, String input, PrintWriter writer){
        logger.info("I'm removing here");
        if (input.split(" ").length == 2){
            String filename = input.split(" ")[1];
            for (int p:dstorePorts) {
                logger.info("THESE ARE THE PORTS DANG IT: "+p);
            }

            if (dstorePorts.size() < repFactor){
                writer.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                writer.flush();
                logger.info("Sending NOT ENOUGH DSTORES");
                removing.set(false);
                return;
            }

            if (!filesStored.containsKey(filename)){
                writer.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                writer.flush();
                logger.info("Sending FILE DOESNT EXIST");
                return;
            }

            List<Integer> list = filesStored.get(filename);
            if (list != null /*&& fileStoreComplete.get(filename)*/){
                for (Integer port:list) {
                    new Thread(()->{
                        try {
                            PrintWriter out = new PrintWriter(dstorePortsSockets.get(port).getOutputStream(),true);
                            out.println(Protocol.REMOVE_TOKEN + " " + filename);
                            out.flush();
                            logger.info("Sending REMOVE_TOKEN to client??");
                        } catch (Exception e) {

                        }
                    }).start();
                }
                fileSocket.put(filename, socket);
                fileAckCount.put(filename, 0);
                if(!fileRemoveComplete.contains(filename)){
                    fileRemoveComplete.add(filename);
                }
                //fileStoreComplete.put(filename,false);
            } else { logger.info("List empty"); }
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.schedule(()->{
                if (fileAckCount.containsKey(filename) && !removing.get()) {
                    //fileStoreComplete.put(filename,true);
                    fileAckCount.remove(filename);
                    fileSocket.remove(filename);
                    logger.info("Timed out remove");
                }
            },timeout, TimeUnit.MILLISECONDS);
        }
    }

    private synchronized void handleREMOVE_ACK(String input) {
        if (input.split(" ").length == 2){
            String filename = input.split(" ")[1];

            List<Integer> list = filesStored.get(filename);
            Socket s = fileSocket.get(filename);
            logger.info("Handling REMOVE_ACK" + fileAckCount.get(filename));
            if (list != null && s != null) {
                synchronized (fileAckCount){
                    fileAckCount.put(filename, fileAckCount.get(filename) + 1);
                    int expectedAcks = filesStored.get(filename).size();
                    if (fileAckCount.get(filename) == expectedAcks) {
                        filesStored.remove(filename);
                        fileAckCount.remove(filename);
                        fileStoreComplete.remove(filename);
                        fileRemoveComplete.remove(filename);
                        fileSizes.remove(filename);
                        removing.set(false);
                        try{
                            PrintWriter writer = new PrintWriter(s.getOutputStream());
                            writer.println(Protocol.REMOVE_COMPLETE_TOKEN);
                            writer.flush();
                            logger.info("Sending REMOVE_COMPLETE");
                        } catch (Exception e){

                        }
                        fileSocket.remove(filename);
                    }
                }

            } else {
                logger.info("List is null? "+ (list == null) + " and the socket: "+(s==null));
            }
        }
    }

    private synchronized void handleLIST(Socket socket, PrintWriter writer){

        if (dstorePorts.size() < repFactor){
            writer.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            writer.flush();
            return;
        }

        String output = Protocol.LIST_TOKEN;
        if (!filesStored.isEmpty()){
            synchronized (filesStored){
                for (String file : filesStored.keySet()) {
                    if (fileStoreComplete.getOrDefault(file, false)&& !fileRemoveComplete.contains(file)) {
                        output = output + " " + file;
                    }
                }
            }

        }
        writer.println(output);
        writer.flush();
        logger.info("Listed: "+output);
    }
}