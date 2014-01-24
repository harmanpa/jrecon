package com.github.harmanpa.jrecon.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A utility class that represents an expandable {@link ByteBuffer}. This class
 * is a decorated ByteBuffer. There are two ways to create an instance of this
 * class: allocate a new buffer with a default amount of bytes (default
 * constructor), or to wrap around a different ByteBuffer instance. <p> The
 * internal ByteBuffer is always in write-mode (it's never flipped for reading,
 * except in {@link #getWritableBuffer()} and expands itself if data needs to be
 * written and the buffer is full. The buffer is expanded by a default amount of
 * bytes (specified by {@link #BUFFER_SIZE}). </p> <p> Custom
 * implementations/overriding methods are welcome! </p> <strong>This utility is
 * not thread-safe!</strong> <p></p>
 * 
 * @author Martin Tuskevicius
 * @author Peter Harman
 * @since Sep 8, 2010 10:55:45 PM
 * 
 */
public class ExpandableByteBuffer {
    /**
     * The default size of the buffer. This will be used in only two situations:
     * to allocate a brand new buffer (in the constructor), or to expand an
     * existing buffer by the amount specified by this constant.
     */
    private static final int BUFFER_SIZE = 1024;
    private final Factory factory;
    private ByteBuffer buf;

    /**
     * Creates a new expandable {@link ByteBuffer} by allocating a new buffer
     * with the default buffer size
     */
    public ExpandableByteBuffer(Factory factory) {
        this.factory = factory;
	buf = factory.create(BUFFER_SIZE);
    }

    /**
     * Creates a new expandable {@link ByteBuffer}, initializing the ByteBuffer
     * instance stored using the provided one.
     * 
     * @param buf
     */
    public ExpandableByteBuffer(ByteBuffer buf) {
	this.buf = buf;
        this.factory = new DefaultFactory(buf.isDirect(), buf.order());
    }

    /**
     * Puts a single byte into the buffer.
     * 
     * @param b
     */
    public void put(int b) {
	verifySize(1);
	buf.put((byte) b);
    }

    /**
     * Puts an array of bytes into the buffer.
     * 
     * @param bytes
     */
    public void put(byte[] bytes) {
	verifySize(bytes.length);
	buf.put(bytes);
    }

    /**
     * Puts an array of bytes from the specified offset into the buffer.
     * 
     * @param bytes
     * @param offset
     * @param length
     */
    public void put(byte[] bytes, int offset, int length) {
	verifySize(length);
	buf.put(bytes, offset, length);
    }

    /**
     * Puts the data from another {@link ByteBuffer} into this buffer.
     * 
     * @param from
     */
    public void put(ByteBuffer from) {
	verifySize(from.capacity() - from.remaining());
	buf.put(from);
    }

    /**
     * Puts a character into the the buffer.
     * 
     * @param c
     */
    public void putCharacter(char c) {
	verifySize(2);
	buf.putChar(c);
    }

    /**
     * Puts a short into the buffer.
     * 
     * @param s
     */
    public void putShort(short s) {
	verifySize(2);
	buf.putShort(s);
    }

    /**
     * Puts an integer into the buffer.
     * 
     * @param i
     */
    public void putInteger(int i) {
	verifySize(4);
	buf.putInt(i);
    }

    /**
     * Puts a float into the buffer.
     * 
     * @param f
     */
    public void putFloat(float f) {
	verifySize(4);
	buf.putFloat(f);
    }

    /**
     * Puts a double into the buffer.
     * 
     * @param d
     */
    public void putDouble(double d) {
	verifySize(8);
	buf.putDouble(d);
    }

    /**
     * Puts a long into the buffer.
     * 
     * @param l
     */
    public void putLong(long l) {
	verifySize(8);
	buf.putLong(l);
    }
/**
     * Puts a String into the the buffer.
     * 
     * @param c
     */
    public void putString(String c) {
	verifySize(2*c.length());
        for(int i=0;i<c.length();i++) {
            buf.putChar(c.charAt(i));
        }
    }

    /**
     * Puts a short into the buffer.
     * 
     * @param s
     */
    public void putShortArray(short[] s) {
	verifySize(2*s.length);
        for(int i=0;i<s.length;i++) {
            buf.putShort(s[i]);
        }
    }

    /**
     * Puts an integer into the buffer.
     * 
     * @param i
     */
    public void putIntegerArray(int[] i) {
	verifySize(4*i.length);
        for(int z=0;z<i.length;z++) {
            buf.putInt(i[z]);
        }
    }

    /**
     * Puts a float into the buffer.
     * 
     * @param f
     */
    public void putFloatArray(float[] f) {
	verifySize(4*f.length);
        for(int i=0;i<f.length;i++) {
            buf.putFloat(f[i]);
        }
    }

    /**
     * Puts a double into the buffer.
     * 
     * @param d
     */
    public void putDoubleArray(double[] d) {
	verifySize(8*d.length);
        for(int i=0;i<d.length;i++) {
            buf.putDouble(d[i]);
        }
    }

    /**
     * Puts a long into the buffer.
     * 
     * @param l
     */
    public void putLongArray(long[] l) {
	verifySize(8*l.length);
        for(int i=0;i<l.length;i++) {
            buf.putLong(l[i]);
        }
    }
    /**
     * Retrieves a read-only version of this buffer. <p> <i>Trying to manually
     * prepare this buffer for writing is discouraged. If this buffer needs to
     * be written, please use {@link #getWritableBuffer()}.</i> </p>
     * 
     * @return the buffer
     */
    public ByteBuffer getByteBuffer() {
	return buf.asReadOnlyBuffer();
    }

    /**
     * Retrieves the writable version of this buffer. All the bytes from the
     * internal {@link ByteBuffer} are taken and put into a byte array (with the
     * size of the {@link ByteBuffer#position()}, and then the array is wrapped
     * and returned (<tt>return ByteBuffer.wrap(array)</tt>). <p> <i>This is an
     * optional method, this is just more convenient than doing so manually!</i>
     * </p>
     * 
     * @return a writable version of this buffer
     */
    public ByteBuffer getWritableBuffer() {
	byte[] data = new byte[buf.position()];
	buf.get(data);
	return ByteBuffer.wrap(data);
    }

    /**
     * Checks if more space needs to be allocated.
     */
    private void verifySize(int amount) {
	if (buf.remaining() >= amount) {
	    return;
	}
	ByteBuffer buf2 = ByteBuffer.allocate(this.buf.capacity() + BUFFER_SIZE);
	buf2.put(this.buf);
	this.buf = buf2;
    }
    
    public interface Factory {
        public ByteBuffer create(int size);
    }
    
    public static class DefaultFactory implements Factory {
        private final boolean direct;
        private final ByteOrder byteOrder;

        public DefaultFactory(boolean direct, ByteOrder byteOrder) {
            this.direct = direct;
            this.byteOrder = byteOrder;
        }

        @Override
        public ByteBuffer create(int size) {
            if(direct) {
                return ByteBuffer.allocateDirect(size).order(byteOrder);
            } else {
                return ByteBuffer.allocate(size).order(byteOrder);
            }
        }
        
    }
}
