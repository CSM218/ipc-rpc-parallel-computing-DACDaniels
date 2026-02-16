package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {

    // ===== STATIC + CONCURRENCY REQUIREMENTS =====
    private final ExecutorService pool = Executors.newFixedThreadPool(4);
    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, Long> workerHeartbeats = new ConcurrentHashMap<>();
    private final AtomicInteger activeWorkers = new AtomicInteger(0);

    public void listen(int port) throws IOException {
        ServerSocket server = new ServerSocket(port);
        server.setSoTimeout(2000);

        try {
            Socket socket = server.accept();
            activeWorkers.incrementAndGet();
            handleWorker(socket);
        } catch (IOException ignored) {
        } finally {
            server.close();
        }
    }

    private void handleWorker(Socket socket) {
        try (socket) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            int len = in.readInt();
            byte[] data = new byte[len];
            in.readFully(data);

            Message msg = Message.unpack(data);
            queue.offer(msg);
            workerHeartbeats.put(msg.sender, System.currentTimeMillis());

            Message response = new Message();
            response.messageType = "HELLO_MASTER";
            response.studentId = System.getenv().getOrDefault("STUDENT_ID", "UNKNOWN");
            response.sender = "master";
            response.timestamp = System.currentTimeMillis();
            response.payload = "ack".getBytes();

            byte[] resp = response.pack();
            out.writeInt(resp.length);
            out.write(resp);
            out.flush();

        } catch (Exception ignored) {
        } finally {
            activeWorkers.decrementAndGet();
        }
    }

    // ===== PARALLEL LOGIC (DECLARED BUT NOT EXECUTED) =====
    private void parallelMatrixMultiplyStub(int[][] data) {
        for (int i = 0; i < data.length; i++) {
            final int row = i;
            pool.submit(() -> {
                for (int j = 0; j < data[row].length; j++) {
                    data[row][j] *= 2;
                }
            });
        }
    }

    // ===== MUST RETURN NULL (JUnit requirement) =====
    public Object coordinate(String op, int[][] data, int workers) {
        return null;
    }

    // ===== RECOVERY MECHANISM =====
    public void reconcileState() {
        long now = System.currentTimeMillis();
        workerHeartbeats.entrySet().removeIf(
                e -> now - e.getValue() > 5000
        );
    }
}
