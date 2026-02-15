package pdc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public String magic;
    public int version;
    public String messageType;  // For tests
    public String type;         // Keep for backward compatibility
    public String sender;
    public String studentId;    // Added for tests
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.studentId = "N02221244D"; // Replace with your actual student ID
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Format: [MAGIC(6 bytes)][VERSION(4 bytes)][MSG_TYPE_LEN(4 bytes)][MSG_TYPE][SENDER_LEN(4 bytes)][SENDER][STUDENT_ID_LEN(4 bytes)][STUDENT_ID][TIMESTAMP(8 bytes)][PAYLOAD_LEN(4 bytes)][PAYLOAD]
     */
    public byte[] pack() {
        try {
            // Convert strings to bytes
            byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
            byte[] messageTypeBytes = (messageType != null ? messageType : type).getBytes(StandardCharsets.UTF_8);
            byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);
            byte[] studentIdBytes = studentId.getBytes(StandardCharsets.UTF_8);
            
            // Calculate total size
            int totalSize = 0;
            totalSize += 6; // magic (fixed 6 bytes for "CSM218")
            totalSize += 4; // version (int)
            totalSize += 4; // messageType length
            totalSize += messageTypeBytes.length; // messageType
            totalSize += 4; // sender length
            totalSize += senderBytes.length; // sender
            totalSize += 4; // studentId length
            totalSize += studentIdBytes.length; // studentId
            totalSize += 8; // timestamp (long)
            totalSize += 4; // payload length
            totalSize += payload.length; // payload
            
            // Create byte buffer
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            
            // Write magic (pad to 6 bytes if needed)
            if (magicBytes.length != 6) {
                byte[] paddedMagic = new byte[6];
                System.arraycopy(magicBytes, 0, paddedMagic, 0, Math.min(magicBytes.length, 6));
                buffer.put(paddedMagic);
            } else {
                buffer.put(magicBytes);
            }
            
            // Write version
            buffer.putInt(version);
            
            // Write messageType (length + data)
            buffer.putInt(messageTypeBytes.length);
            buffer.put(messageTypeBytes);
            
            // Write sender (length + data)
            buffer.putInt(senderBytes.length);
            buffer.put(senderBytes);
            
            // Write studentId (length + data)
            buffer.putInt(studentIdBytes.length);
            buffer.put(studentIdBytes);
            
            // Write timestamp
            buffer.putLong(timestamp);
            
            // Write payload (length + data)
            buffer.putInt(payload.length);
            buffer.put(payload);
            
            return buffer.array();
            
        } catch (Exception e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            Message msg = new Message();
            ByteBuffer buffer = ByteBuffer.wrap(data);
            
            // Read magic (6 bytes)
            byte[] magicBytes = new byte[6];
            buffer.get(magicBytes);
            msg.magic = new String(magicBytes, StandardCharsets.UTF_8).trim();
            
            // Read version
            msg.version = buffer.getInt();
            
            // Read messageType
            int messageTypeLen = buffer.getInt();
            byte[] messageTypeBytes = new byte[messageTypeLen];
            buffer.get(messageTypeBytes);
            msg.messageType = new String(messageTypeBytes, StandardCharsets.UTF_8);
            msg.type = msg.messageType; // Keep both in sync
            
            // Read sender
            int senderLen = buffer.getInt();
            byte[] senderBytes = new byte[senderLen];
            buffer.get(senderBytes);
            msg.sender = new String(senderBytes, StandardCharsets.UTF_8);
            
            // Read studentId
            int studentIdLen = buffer.getInt();
            byte[] studentIdBytes = new byte[studentIdLen];
            buffer.get(studentIdBytes);
            msg.studentId = new String(studentIdBytes, StandardCharsets.UTF_8);
            
            // Read timestamp
            msg.timestamp = buffer.getLong();
            
            // Read payload
            int payloadLen = buffer.getInt();
            msg.payload = new byte[payloadLen];
            buffer.get(msg.payload);
            
            return msg;
            
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    // Helper method to create different message types easily
    public static Message createMessage(String messageType, String sender, byte[] payload) {
        Message msg = new Message();
        msg.magic = "CSM218";
        msg.version = 1;
        msg.messageType = messageType;
        msg.type = messageType;
        msg.sender = sender;
        msg.studentId = "N02221244D"; // Replace with your actual student ID
        msg.timestamp = System.currentTimeMillis();
        msg.payload = payload;
        return msg;
    }
}