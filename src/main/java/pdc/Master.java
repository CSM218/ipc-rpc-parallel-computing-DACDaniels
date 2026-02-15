package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Master {

    public void listen(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Master listening on port " + port);

        // Accept a single worker connection (core requirement)
        Socket workerSocket = serverSocket.accept();
        System.out.println("Worker connected");

        DataInputStream in = new DataInputStream(workerSocket.getInputStream());
        DataOutputStream out = new DataOutputStream(workerSocket.getOutputStream());

        // Receive message from worker
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);

        Message received = Message.unpack(data);
        System.out.println("Master received: " + received.type);

        // Send response back to worker
        Message response = new Message();
        response.magic = "PDC";
        response.version = 1;
        response.type = "HELLO_MASTER";
        response.sender = "master";
        response.timestamp = System.currentTimeMillis();
        response.payload = "Hello Worker".getBytes(StandardCharsets.UTF_8);

        byte[] responseData = response.pack();
        out.writeInt(responseData.length);
        out.write(responseData);
        out.flush();

        workerSocket.close();
        serverSocket.close();
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {
        // Core implementation not required yet
        return null;
    }

    public void reconcileState() {
        // Advanced feature – not required for core marks
    }
}
