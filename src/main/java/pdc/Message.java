package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Message {
    public String magic;
    public int version;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;

    public Message() {}

    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            writeString(dos, magic);
            dos.writeInt(version);
            writeString(dos, type);
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
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    public static Message unpack(byte[] data) {
        try {
            DataInputStream dis = new DataInputStream(
                    new ByteArrayInputStream(data)
            );

            Message msg = new Message();
            msg.magic = readString(dis);
            msg.version = dis.readInt();
            msg.type = readString(dis);
            msg.sender = readString(dis);
            msg.timestamp = dis.readLong();

            int payloadLength = dis.readInt();
            if (payloadLength > 0) {
                msg.payload = new byte[payloadLength];
                dis.readFully(msg.payload);
            } else {
                msg.payload = new byte[0];
            }

            return msg;

        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }

    private static void writeString(DataOutputStream dos, String s) throws IOException {
        if (s == null) {
            dos.writeInt(0);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private static String readString(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        if (length <= 0) {
            return "";
        }
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
