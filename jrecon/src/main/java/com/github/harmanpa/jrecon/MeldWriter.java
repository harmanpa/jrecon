/*
 * The MIT License
 *
 * Copyright 2014 CyDesign Limited
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.FinalizedException;
import com.github.harmanpa.jrecon.exceptions.ReconException;
import com.github.harmanpa.jrecon.exceptions.TransposedException;
import com.github.harmanpa.jrecon.utils.Compression;
import com.github.harmanpa.jrecon.utils.ExpandableByteBuffer;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import org.msgpack.packer.BufferPacker;

/**
 *
 * @author pete
 */
public class MeldWriter extends ReconWriter {

    private static final String MELD_ID = "recon:meld:v01";
    private boolean definitionsDirty = false;
    private int maximumHeaderSize;
    private RandomAccessFile raf;
    private final boolean comp;

    public MeldWriter(File file, boolean compressed) {
        super(file);
        this.comp = compressed;
    }

    public MeldWriter(File file) {
        this(file, false);
    }

    @Override
    protected ReconTable createTable(String name, Iterable<String> signals) {
        return new MeldTableWriter(name, signals);
    }

    @Override
    protected ReconObject createObject(String name) {
        return new MeldObjectWriter(name);
    }

    /**
     * This method is called when all tables and objects have been defined. Once
     * called, it is not possible to add new tables or objects. Furthermore, it
     * is not possible to add rows or fields until the wall has been finalized.
     *
     * @throws java.io.IOException
     */
    @Override
    public final void finalizeDefinitions() throws IOException {
        if (defined && !definitionsDirty) {
            return;
        }
        int lengthDifference = 0;
        bufferPacker.writeMapBegin(3);
        // Write file meta
        bufferPacker.write("fmeta");
        bufferPacker.write(getFileMeta());
        // Write table definitions
        bufferPacker.write("tabs");
        bufferPacker.writeMapBegin(getTables().size());
        for (ReconTable table : getTables().values()) {
            bufferPacker.write(table.getName());
            bufferPacker.writeMapBegin(4);
            bufferPacker.write("tmeta");
            bufferPacker.write(table.getTableMeta());
            bufferPacker.write("vars");
            bufferPacker.writeArrayBegin(table.getSignals().length);
            for (String signal : table.getSignals()) {
                bufferPacker.write(signal);
            }
            bufferPacker.writeArrayEnd();
            bufferPacker.write("toff");
            bufferPacker.writeMapBegin(table.getSignals().length);
            for (String signal : table.getSignals()) {
                OffsetLength ol = ((MeldTableWriter) table).getSignalOffsetLength(signal);
                String transform = ((MeldTableWriter) table).getSignalTransform(signal);
                bufferPacker.write(signal);
                bufferPacker.writeMapBegin(3);
                bufferPacker.write("i");
                lengthDifference += writeIntegerByteDifference(bufferPacker, ol.getOffset());
                bufferPacker.write("l");
                lengthDifference += writeIntegerByteDifference(bufferPacker, ol.getLength());
                bufferPacker.write("t");
                bufferPacker.write(transform);
                bufferPacker.writeMapEnd();
            }
            bufferPacker.writeMapEnd();
            bufferPacker.write("vmeta");
            bufferPacker.writeMapBegin(table.getSignals().length);
            for (String s : table.getSignals()) {
                bufferPacker.write(s);
                bufferPacker.write(table.getSignalMeta(s));
            }
            bufferPacker.writeMapEnd();
            bufferPacker.writeMapEnd();
        }
        bufferPacker.writeMapEnd();
        // Write object definitions
        bufferPacker.write("objs");
        bufferPacker.writeMapBegin(getObjects().size());
        for (ReconObject object : getObjects().values()) {
            OffsetLength ol = ((MeldObjectWriter) object).getOffsetLength();
            bufferPacker.write(object.getName());
            bufferPacker.writeMapBegin(3);
            bufferPacker.write("ometa");
            bufferPacker.write(object.getObjectMeta());
            bufferPacker.write("i");
            lengthDifference += writeIntegerByteDifference(bufferPacker, ol.getOffset());
            bufferPacker.write("l");
            lengthDifference += writeIntegerByteDifference(bufferPacker, ol.getLength());
            bufferPacker.writeMapEnd();
        }
        bufferPacker.writeMapEnd();
        bufferPacker.writeMapEnd();
        int variableHeaderSize = bufferPacker.getBufferSize();
        if (defined) {
            if (maximumHeaderSize != MELD_ID.getBytes().length + 4 + variableHeaderSize + lengthDifference) {
                throw new IOException("Incorrectly sized header");
            }
            RandomAccessFile randomAccessFile = getRandomAccessFile();
            long pointer = randomAccessFile.getFilePointer();            
            ByteBuffer bb = ByteBuffer.allocate(maximumHeaderSize);
            bb.put(MELD_ID.getBytes());
            bb.putInt(variableHeaderSize);
            bb.put(bufferPacker.toByteArray());
            for (int i = 0; i < lengthDifference; i++) {
                bb.put((byte) 0x00);
            }            
            if (pointer >= maximumHeaderSize) {
                randomAccessFile.seek(0L);
                randomAccessFile.write(bb.array(), 0, maximumHeaderSize);
                randomAccessFile.seek(pointer);
            } else {
                buffer.put(0, bb.array(), 0, maximumHeaderSize);
            }
        } else {
            maximumHeaderSize = MELD_ID.getBytes().length + 4 + variableHeaderSize + lengthDifference;
            // Buffer fixed header
            buffer.put(MELD_ID.getBytes());
            buffer.putInteger(variableHeaderSize);
            // Buffer variable header
            buffer.put(bufferPacker.toByteArray());
            pad(buffer, lengthDifference);
            defined = true;
        }
        bufferPacker.clear();
    }

    private void setDefinitionsDirty() {
        definitionsDirty = true;
    }

    public boolean isCompressed() {
        return comp;
    }

    private void pad(ExpandableByteBuffer buffer, int n) {
        for (int i = 0; i < n; i++) {
            buffer.put((byte) 0x00);
        }
    }

    private RandomAccessFile getRandomAccessFile() throws IOException {
        if (raf == null) {
            raf = new RandomAccessFile(file, "rw");
        }
        return raf;
    }

    @Override
    public void flush() throws IOException {
        for (ReconObject object : getObjects().values()) {
            MeldObjectWriter objectWriter = (MeldObjectWriter) object;
            if (objectWriter.getOffsetLength().getOffset() != 0 && objectWriter.getOffsetLength().getLength() != 0) {
                try {
                    objectWriter.writeData();
                } catch (ReconException ex) {
                    throw new IOException(ex);
                }
            }
        }
        if (definitionsDirty) {
            finalizeDefinitions();
        }
        buffer.writeToRandomAccessFile(getRandomAccessFile());
    }

    private int writeIntegerByteDifference(BufferPacker packer, int value) throws IOException {
        int start = packer.getBufferSize();
        packer.write(value);
        int diff = 5 - (packer.getBufferSize() - start);
        return diff;
    }

    private int offset() throws IOException {
        int offset = buffer.position();
        if (raf != null) {
            offset += (int) raf.getFilePointer();
        }
        return offset;
    }

    class MeldTableWriter extends ReconTableWriter {

        private final Map<String, OffsetLength> offsetLengths;
        private final Map<String, String> transforms;

        public MeldTableWriter(String name, Iterable<String> signals) {
            super(name, signals);
            this.offsetLengths = new HashMap<String, OffsetLength>();
            this.transforms = new HashMap<String, String>();
        }

        @Override
        public final void addRow(Object... data) throws ReconException {
            throw new TransposedException();
        }

        @Override
        public final void addAlias(final String alias, String of, String transform) throws ReconException {
            checkNotFinalized();
            checkSignalExistence(of, true);
            checkSignalExistence(alias, false);
            addSignal(alias);
            transforms.put(alias, transform);
        }

        @Override
        public void setSignal(String signal, Object... data) throws ReconException {
            checkFinalized();
            if (offsetLengths.containsKey(signal) && offsetLengths.get(signal).getLength() == 0 && offsetLengths.get(signal).getOffset() == 0) {
                throw new FinalizedException("Signal already written");
            }
            try {
                int offset = offset();
                BufferPacker packer = getMessagePack().createBufferPacker();
                packer.writeArrayBegin(data.length);
                for (Object element : data) {
                    packer.write(element);
                }
                packer.writeArrayEnd();
                byte[] bytes = packer.toByteArray();
                if (isCompressed()) {
                    bytes = Compression.compress(bytes);
                }
                offsetLengths.put(signal, new OffsetLength(offset, bytes.length));
                buffer.put(bytes);
                setDefinitionsDirty();
            } catch (IOException ex) {
                throw new ReconException("Could not write signal " + getName(), ex);
            }
        }

        @Override
        public Alias[] getAliases() {
            return new Alias[0];
        }

        @Override
        public String[] getVariables() {
            return signals.toArray(new String[0]);
        }

        protected OffsetLength getSignalOffsetLength(String signal) {
            if (offsetLengths.containsKey(signal)) {
                return offsetLengths.get(signal);
            }
            return new OffsetLength(0, 0);
        }

        protected String getSignalTransform(String signal) {
            if (transforms.containsKey(signal)) {
                return transforms.get(signal);
            }
            return "";
        }
    }

    class MeldObjectWriter extends ReconObjectWriter {

        private final Map<String, Object> fieldData;
        private OffsetLength ol;

        public MeldObjectWriter(String name) {
            super(name);
            this.fieldData = new HashMap<String, Object>();
        }

        @Override
        public void addField(String name, Object value) throws ReconException {
            checkNotFinalized();
            fieldData.put(name, value);
        }

        @Override
        public Map<String, Object> getFields() throws ReconException {
            return ImmutableMap.copyOf(fieldData);
        }

        protected void writeData() throws ReconException {
            checkFinalized();
            if (ol != null && ol.getLength() == 0 && ol.getOffset() == 0) {
                throw new FinalizedException("Object already written");
            }
            try {
                int offset = offset();
                BufferPacker packer = getMessagePack().createBufferPacker();
                packer.writeMapBegin(fieldData.size());
                for (Map.Entry<String, Object> entry : fieldData.entrySet()) {
                    packer.write(entry.getKey());
                    packer.write(entry.getValue());
                }
                packer.writeMapEnd();
                byte[] bytes = packer.toByteArray();
                if (isCompressed()) {
                    bytes = Compression.compress(bytes);
                }
                ol = new OffsetLength(offset, bytes.length);
                buffer.put(bytes);
                setDefinitionsDirty();
            } catch (IOException ex) {
                throw new ReconException("Could not write object " + getName(), ex);
            }
        }

        protected OffsetLength getOffsetLength() {
            if (ol == null) {
                return new OffsetLength(0, 0);
            }
            return ol;
        }
    }
}
