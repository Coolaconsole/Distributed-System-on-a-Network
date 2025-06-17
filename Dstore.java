import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.logging.Logger;

public class Dstore {
    private static final Logger logger = Logger.getLogger(Controller.class.getName());

    private final int port;
    private final int cport; //controller port
    private final int timeout; //milliseconds
    private final String fileFolder;
    private Socket cSocket; //controller socket
    private PrintWriter cWriter; //controller writer
    private BufferedReader cReader; //controller reader
    private final HashMap<String,Integer> fileSizes;

    public Dstore(int port, int cport, int timeout, String fileFolder){
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = fileFolder;

        this.fileSizes = new HashMap<>();
    }

    public static void main(String[] args){
        if (args.length != 4){
            System.out.println("Invalid arguments");
            return;
        }

        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String fileFolder = args[3];

        Dstore dstore = new Dstore(port,cport,timeout,fileFolder);
        dstore.start();
    }

    public void start(){
        while (true){
            try{
                Socket socket = new Socket(InetAddress.getLocalHost(), cport);
                PrintWriter writer = new PrintWriter(socket.getOutputStream());
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                writer.println("JOIN "+port);
                writer.flush();
                logger.info("Sending JOIN");

                cSocket = socket;
                cWriter = writer;
                cReader = reader;

                break;
            } catch (Exception e){
                //cWriter.println(Protocol.ERROR_LOAD_TOKEN);
            }
        }
        new Thread(this::requestsFromController).start();
        new Thread(this::requestsFromClient).start();
    }

    public void requestsFromController() {

        while (true) {

            try {
                String input = cReader.readLine();
                new Thread(() -> handleControllerRequests(cSocket,input)).start();
            } catch (IOException ignored) {

            }

        }
    }

    public void requestsFromClient() {

        while (true) {

            try (ServerSocket serverSocket = new ServerSocket(port)) {
                while (true) {
                    Socket socket = serverSocket.accept();
                    new Thread(() -> handleClientRequests(socket)).start();
                }

            } catch (IOException ignored) {

            }

        }
    }

    private void handleControllerRequests(Socket socket, String input){
        if (input.startsWith(Protocol.REMOVE_TOKEN)) {
            logger.info("Received REMOVE");
            new Thread(()->handleREMOVE(input)).start();
        } else if (input.startsWith(Protocol.LIST_TOKEN)) {

            new Thread(this::handleLIST).start();
        } else if (input.startsWith(Protocol.REBALANCE_TOKEN)) {

            //new Thread(()->handleREBALANCE(input)).start();
        } else {
            //break;
        }

    }

    private void handleClientRequests(Socket socket){
        try {
            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();
            BufferedReader inReader = new BufferedReader(new InputStreamReader(input));

            String line = inReader.readLine();
            //DstoreLogger.getInstance().messageReceived(socket, sentence);
            if (line.startsWith(Protocol.STORE_TOKEN)) {
                logger.info("Received STORE");
                new Thread(()->handleSTORE(socket, input, output, line)).start();
            } else if (line.startsWith(Protocol.LOAD_DATA_TOKEN)) {
                logger.info("Received LOAD_DATA");
                new Thread(()->handleLOAD_DATA(socket, input, output, line)).start();
            } else if (line.startsWith(Protocol.REBALANCE_STORE_TOKEN)) {

                //new Thread(()->handleREBALANCE_STORE(socket, input, output, line)).start();
            } else  if (line.startsWith(Protocol.REMOVE_TOKEN)) {
                logger.info("Received REMOVE FROM CLIENT!?!?!?!??!?!");
                new Thread(()->handleREMOVE(line)).start();
            } else {
                //break;
            }

        } catch (Exception ignored) {
        }

    }

    private void handleREMOVE(String input) {
        if (input.split(" ").length == 2){
            String filename = input.split(" ")[1];
            File file = new File(fileFolder, filename);
            if (file.exists()){
                file.delete();
                synchronized (cWriter){
                    cWriter.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
                    cWriter.flush();
                    logger.info("Sending REMOVE_ACK");
                }
            } else{
                synchronized (cWriter) {
                    cWriter.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
                    cWriter.flush();
                }
            }

        }
    }

    private void handleLIST() {
        File file = new File(fileFolder);
        String[] fileList = file.list();
        if (fileList!=null){
            String output = Protocol.LIST_TOKEN;
            for (String f:fileList) {
                output = output + " " + f;
            }
            synchronized (cWriter){
                cWriter.println(output);
                cWriter.flush();
                logger.info("Listed "+output);
            }
        }
    }

    private void handleSTORE(Socket socket, InputStream input, OutputStream output, String line) {
        if (line.split(" ").length == 3){
            String filename = line.split(" ")[1];
            int filesize = Integer.parseInt(line.split(" ")[2]);

            try (PrintWriter writer = new PrintWriter(output)){
                writer.println(Protocol.ACK_TOKEN);
                writer.flush();
                logger.info("Sending ACK");

                fileSizes.put(filename, filesize);

                byte[] data = socket.getInputStream().readNBytes(filesize);
                Files.write(new File(fileFolder, filename).toPath(),data);
                synchronized (cWriter){
                    cWriter.println(Protocol.STORE_ACK_TOKEN+ " " + filename);
                    cWriter.flush();
                    logger.info("Sending STORE_ACK "+filename);
                }

            } catch (Exception ignored){

            }
        }
    }

    private void handleLOAD_DATA(Socket socket, InputStream input, OutputStream output, String line) {
        if (line.split(" ").length == 2){
            String filename = line.split(" ")[1];
            File file = new File(fileFolder, filename);
            try (PrintWriter writer = new PrintWriter(output); FileInputStream fileInputStream = new FileInputStream(file)){

                if (file.exists()){
                    byte[] bytes = new byte[1024];
                    int temp;
                    while ((temp = fileInputStream.read(bytes)) != -1){
                        output.write(bytes,0,temp);
                    }
                }
            } catch (Exception ignored){

            }
        }
    }
}
