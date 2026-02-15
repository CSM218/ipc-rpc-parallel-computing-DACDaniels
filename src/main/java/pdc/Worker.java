package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Worker {

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());

            // Send HELLO message to Master
            Message hello = new Message();
            hello.magic = "PDC";
            hello.version = 1;
            hello.type = "HELLO_WORKER";
            hello.sender = "worker";
            hello.timestamp = System.currentTimeMillis();
            hello.payload = "Hello Master".getBytes(StandardCharsets.UTF_8);

            sendMessage(hello);

            // Wait for response
            Message response = receiveMessage();
            System.out.println("Worker received: " + response.type);

        } catch (IOException e) {
            throw new RuntimeException("Worker failed to join cluster", e);
        }
    }

    public void execute() {
        // Execution logic will be added later (matrix work)
        // For now, IPC validation is sufficient
    }

    private void sendMessage(Message msg) throws IOException {
        byte[] data = msg.pack();
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }

    private Message receiveMessage() throws IOException {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        return Message.unpack(data);
    }
}
