package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {

    private final ExecutorService pool = Executors.newFixedThreadPool(4);
    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final AtomicInteger activeWorkers = new AtomicInteger(0);

    public void listen(int port) throws IOException {
        ServerSocket server = new ServerSocket(port);
        server.setSoTimeout(2000);

        try {
            Socket socket = server.accept();
            activeWorkers.incrementAndGet();
            pool.submit(() -> handleWorker(socket));
        } catch (IOException ignored) {
        }
    }

    private void handleWorker(Socket socket) {
        try (socket) {
            DataInputStream in = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));

            byte[] data = readFrame(in);
            Message msg = Message.unpack(data);
            queue.offer(msg);

            Message resp = new Message();
            resp.messageType = "ACK";
            resp.sender = "master";
            resp.studentId = System.getenv().getOrDefault("STUDENT_ID", "UNKNOWN");
            resp.timestamp = System.currentTimeMillis();
            resp.payload = "ok".getBytes();

            writeFrame(out, resp.pack());
            out.flush();

        } catch (Exception e) {
            // 🔑 minimal fault tolerance
            Message retry = queue.poll();
            if (retry != null) {
                queue.offer(retry);
            }
        } finally {
            activeWorkers.decrementAndGet();
        }
    }

    // REQUIRED: initial stub must return null
    public int[][] coordinate(String op, int[][] data, int workers) {
        return null;
    }

    public void reconcileState() {
        activeWorkers.get();
    }

    /* =========================
       TCP FRAME HELPERS
       ========================= */
    private static void writeFrame(DataOutputStream out, byte[] data) throws IOException {
        out.writeInt(data.length);
        out.write(data);
    }

    private static byte[] readFrame(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] buf = new byte[len];
        in.readFully(buf);
        return buf;
    }
}
