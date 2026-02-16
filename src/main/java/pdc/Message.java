package pdc;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Message {

    // ===== REQUIRED FIELDS =====
    public String magic = "PDC";
    public int version = 1;
    public String messageType;
    public String studentId;
    public String sender;
    public long timestamp;
    public byte[] payload;

    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            writeString(dos, magic);
            dos.writeInt(version);
            writeString(dos, messageType);
            writeString(dos, studentId);
            writeString(dos, sender);
            dos.writeLong(timestamp);

            if (payload != null) {
                dos.writeInt(payload.length);
                dos.write(payload);
            } else {
                dos.writeInt(0);
            }

            dos.flush();
            return baos.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Message unpack(byte[] data) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            Message m = new Message();

            m.magic = readString(dis);
            m.version = dis.readInt();
            m.messageType = readString(dis);
            m.studentId = readString(dis);
            m.sender = readString(dis);
            m.timestamp = dis.readLong();

            int len = dis.readInt();
            if (len > 0) {
                m.payload = new byte[len];
                dis.readFully(m.payload);
            }

            return m;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeString(DataOutputStream dos, String s) throws IOException {
        if (s == null) {
            dos.writeInt(0);
            return;
        }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(b.length);
        dos.write(b);
    }

    private static String readString(DataInputStream dis) throws IOException {
        int len = dis.readInt();
        if (len == 0) return "";
        byte[] b = new byte[len];
        dis.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}
