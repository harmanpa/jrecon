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
import com.github.harmanpa.jrecon.exceptions.ReadOnlyException;
import com.github.harmanpa.jrecon.exceptions.ReconException;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.MapValue;
import org.msgpack.type.Value;
import org.msgpack.unpacker.BufferUnpacker;
import org.msgpack.unpacker.Unpacker;

/**
 *
 * @author pete
 */
public abstract class ReconReader extends ReconFile implements Serializable {

    private Map<String, ReconTable> tables;
    private Map<String, ReconObject> objects;
    private Map<String, Object> meta;
    private boolean comp = false;
    private boolean headerRead = false;

    private int readFixedHeader() throws IOException {
        byte[] fixed = readFixedHeaderBytes();
        if (!getFileTypeString().equals(new String(Arrays.copyOf(fixed, 14)))) {
            throw new IOException("Incorrect file type");
        }
        return ByteBuffer.wrap(Arrays.copyOfRange(fixed, 14, 18)).getInt();
    }

    protected abstract String getFileTypeString();

    protected abstract byte[] readFixedHeaderBytes() throws IOException;

    protected abstract byte[] readVariableHeaderBytes(int size) throws IOException;

    protected final void readHeader() throws IOException {
        if (!headerRead) {
            int variableHeaderSize = readFixedHeader();
            byte[] variableHeaderBytes = readVariableHeaderBytes(variableHeaderSize);
            BufferUnpacker unpacker = getMessagePack().createBufferUnpacker(variableHeaderBytes);
            visitHeader(unpacker);
            unpacker.close();
            headerRead = true;
        }
    }

    protected final void visitHeader(Unpacker unpacker) throws IOException {
        int mapLength = unpacker.readMapBegin();
        for (int i = 0; i < mapLength; i++) {
            String name = unpacker.readString();
            if ("fmeta".equals(name)) {
                this.meta = visitMetaMap(unpacker);
            } else if ("tabs".equals(name)) {
                this.tables = visitTableMap(unpacker);
            } else if ("objs".equals(name)) {
                this.objects = visitObjectMap(unpacker);
            } else if ("comp".equals(name)) {
                this.comp = unpacker.readBoolean();
            }
        }
        unpacker.readMapEnd();
        if (this.meta == null) {
            this.meta = ImmutableMap.of();
        }
        if (this.tables == null) {
            this.tables = ImmutableMap.of();
        }
        if (this.objects == null) {
            this.objects = ImmutableMap.of();
        }
    }

    protected final Object readObject(Unpacker unpacker) throws IOException {
        Value value = unpacker.readValue();
        return valueToObject(value);
    }

    protected final Object valueToObject(Value value) throws IOException {
        switch (value.getType()) {
            case ARRAY:
                return arrayToObject(value.asArrayValue());
            case BOOLEAN:
                return value.asBooleanValue().getBoolean();
            case FLOAT:
                return value.asFloatValue().getDouble();
            case INTEGER:
                return value.asIntegerValue().getInt();
            case RAW:
                return new String(value.asRawValue().getByteArray());
            case MAP:
                return mapToObject(value.asMapValue());
            default:
                return null;
        }
    }

    protected final Object arrayToObject(ArrayValue array) throws IOException {
        if (array.isEmpty()) {
            return new Object[0];
        }
        switch (array.getElementArray()[0].getType()) {
            case BOOLEAN:
                Boolean[] out = new Boolean[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    out[i] = array.getElementArray()[i].asBooleanValue().getBoolean();
                }
                return out;
            case FLOAT:
                Double[] out2 = new Double[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    out2[i] = array.getElementArray()[i].asFloatValue().getDouble();
                }
                return out2;
            case INTEGER:
                Integer[] out3 = new Integer[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    out3[i] = array.getElementArray()[i].asIntegerValue().getInt();
                }
                return out3;
            case RAW:
                String[] out4 = new String[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    out4[i] = String.valueOf(array.getElementArray()[i].asRawValue().getString());
                }
                return out4;
            case ARRAY:
                Object[] out5 = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    out5[i] = arrayToObject(array.getElementArray()[i].asArrayValue());
                }
                return out5;
            case MAP:
                Object[] out6 = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    out6[i] = mapToObject(array.getElementArray()[i].asMapValue());
                }
                return out6;
            default:
                return new Object[0];
        }
    }

    protected final Object mapToObject(MapValue map) throws IOException {
        Value[] kv = map.getKeyValueArray();
        if ((kv.length % 2) != 0) {
            throw new IOException("Found map with odd number of keys+values");
        }
        ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
        for (int i = 0; i < kv.length; i = i + 2) {
            builder.put(valueToObject(kv[i]), valueToObject(kv[i + 1]));
        }
        return builder.build();
    }

    protected final Map<String, Object> visitMetaMap(Unpacker unpacker) throws IOException {
        int mapLength = unpacker.readMapBegin();
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        for (int i = 0; i < mapLength; i++) {
            String name = unpacker.readString();
            Object value = readObject(unpacker);
            builder.put(name, value);
        }
        unpacker.readMapEnd();
        return builder.build();
    }

    protected final Map<String, ReconTable> visitTableMap(Unpacker unpacker) throws IOException {
        int mapLength = unpacker.readMapBegin();
        ImmutableMap.Builder<String, ReconTable> builder = ImmutableMap.builder();
        for (int i = 0; i < mapLength; i++) {
            String name = unpacker.readString();
            ReconTable table = visitTable(name, unpacker);
            builder.put(name, table);
        }
        unpacker.readMapEnd();
        return builder.build();
    }

    protected abstract ReconTable visitTable(String name, Unpacker unpacker) throws IOException;

    protected final Map<String, ReconObject> visitObjectMap(Unpacker unpacker) throws IOException {
        int mapLength = unpacker.readMapBegin();
        ImmutableMap.Builder<String, ReconObject> builder = ImmutableMap.builder();
        for (int i = 0; i < mapLength; i++) {
            String name = unpacker.readString();
            ReconObject object = visitObject(name, unpacker);
            builder.put(name, object);
        }
        unpacker.readMapEnd();
        return builder.build();
    }

    protected abstract ReconObject visitObject(String name, Unpacker unpacker) throws IOException;

    @Override
    public final ReconTable addTable(String name, String... signals) throws ReconException {
        throw new FinalizedException();
    }

    @Override
    public final ReconObject addObject(String name) throws ReconException {
        throw new FinalizedException();
    }

    @Override
    public final void addMeta(String name, Object value) throws ReconException {
        throw new FinalizedException();
    }

    @Override
    public final boolean isDefined() {
        return true;
    }

    public boolean isCompressed() {
        return comp;
    }

    @Override
    public final void finalizeDefinitions() throws IOException {
    }

    @Override
    public final Map<String, ReconObject> getObjects() throws ReconException {
        try {
            readHeader();
            return objects;
        } catch (IOException ex) {
            throw new ReconException("Could not read header", ex);
        }
    }

    @Override
    public final Map<String, ReconTable> getTables() throws ReconException {
        try {
            readHeader();
            return tables;
        } catch (IOException ex) {
            throw new ReconException("Could not read header", ex);
        }
    }

    @Override
    public final Map<String, Object> getFileMeta() throws ReconException {
        try {
            readHeader();
            return meta;
        } catch (IOException ex) {
            throw new ReconException("Could not read header", ex);
        }
    }

    @Override
    public final void flush() throws IOException {
    }

    abstract class ReconObjectReader implements ReconObject {

        private final String name;
        private final Map<String, Object> meta;

        public ReconObjectReader(String name, Map<String, Object> meta) {
            this.name = name;
            this.meta = meta;
        }

        @Override
        public final String getName() {
            return name;
        }

        @Override
        public final Map<String, Object> getObjectMeta() {
            return meta;
        }

        @Override
        public final void addField(String name, Object value) throws ReconException {
            throw new ReadOnlyException();
        }

        @Override
        public final void addMeta(String name, Object value) throws ReconException {
            throw new FinalizedException();
        }
    }

    abstract class ReconTableReader implements ReconTable {

        private final String name;
        private final String[] signals;
        private final Map<String, Object> meta;
        private final Map<String, Map<String, Object>> signalMeta;

        public ReconTableReader(String name, String[] signals, Map<String, Object> meta, Map<String, Map<String, Object>> signalMeta) {
            this.name = name;
            this.signals = signals;
            this.meta = meta;
            this.signalMeta = signalMeta;
        }

        @Override
        public final String getName() {
            return name;
        }

        @Override
        public final String[] getSignals() {
            return signals;
        }

        @Override
        public final Map<String, Object> getTableMeta() {
            return meta;
        }

        @Override
        public final Map<String, Object> getSignalMeta(String signal) {
            return signalMeta.containsKey(signal) ? signalMeta.get(signal) : new HashMap<String, Object>();
        }

        @Override
        public final void addRow(Object... data) throws ReconException {
            throw new ReadOnlyException();
        }

        @Override
        public void addSignal(String signal) throws ReconException {
            throw new ReadOnlyException();
        }

        @Override
        public final void addMeta(String name, Object data) throws ReconException {
            throw new FinalizedException();
        }

        @Override
        public final void addSignalMeta(String signal, String name, Object data) throws ReconException {
            throw new FinalizedException();
        }

        @Override
        public final void addAlias(String var, String alias, String transform) throws ReconException {
            throw new FinalizedException();
        }

        @Override
        public final void addAlias(String var, String alias) throws ReconException {
            throw new FinalizedException();
        }

        @Override
        public final void setSignal(String signal, Object... data) throws ReconException {
            throw new ReadOnlyException();
        }
    }
}
