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

import com.github.harmanpa.jrecon.exceptions.ReconException;
import com.github.harmanpa.jrecon.io.RandomAccessResource;
import com.github.harmanpa.jrecon.utils.Compression;
import com.github.harmanpa.jrecon.utils.Transforms;
import com.google.common.collect.ObjectArrays;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.msgpack.MessagePack;
import org.msgpack.unpacker.BufferUnpacker;
import org.msgpack.unpacker.Unpacker;

/**
 *
 * @author pete
 */
public class MeldReader extends ReconReader {

    private final RandomAccessResource resource;

    public MeldReader(RandomAccessResource resource) {
        this.resource = resource;
    }

    @Override
    protected final String getFileTypeString() {
        return "recon:meld:v01";
    }

    @Override
    protected final byte[] readFixedHeaderBytes() throws IOException {
        byte[] fixed = new byte[18];
        if (18 != this.resource.read(0L, fixed)) {
            throw new IOException("Could not read fixed header");
        }
        return fixed;
    }

    @Override
    protected final byte[] readVariableHeaderBytes(int size) throws IOException {
        byte[] variableHeaderBytes = new byte[size];
        if (size != this.resource.read(18L, variableHeaderBytes)) {
            throw new IOException("Could not read variable header");
        }
        return variableHeaderBytes;
    }

    @Override
    protected final ReconTable visitTable(String name, Unpacker unpacker) throws IOException {
        Map<String, Object> tableMeta = new HashMap<String, Object>();
        Map<String, Map<String, Object>> signalMeta = new HashMap<String, Map<String, Object>>();
        List<String> signals = new ArrayList<String>();
        Map<String, OffsetLength> offsets = new HashMap<String, OffsetLength>();
        Map<String, String> transforms = new HashMap<String, String>();
        int mapLength = unpacker.readMapBegin();
        for (int i = 0; i < mapLength; i++) {
            String entryName = unpacker.readString();
            if ("tmeta".equals(entryName)) {
                tableMeta.putAll(visitMetaMap(unpacker));
            } else if ("vars".equals(entryName)) {
                int nSignals = unpacker.readArrayBegin();
                for (int j = 0; j < nSignals; j++) {
                    signals.add(unpacker.readString());
                }
                unpacker.readArrayEnd();
            } else if ("toff".equals(entryName)) {
                int nVariables = unpacker.readMapBegin();
                for (int j = 0; j < nVariables; j++) {
                    String variable = unpacker.readString();
                    int index = 0;
                    int length = 0;
                    String transform = "";
                    int nData = unpacker.readMapBegin();
                    for (int k = 0; k < nData; k++) {
                        String variableData = unpacker.readString();
                        if ("i".equals(variableData)) {
                            index = unpacker.readInt();
                        } else if ("l".equals(variableData)) {
                            length = unpacker.readInt();
                        } else if ("t".equals(variableData)) {
                            transform = unpacker.readString();
                        }
                    }
                    unpacker.readMapEnd();
                    offsets.put(variable, new OffsetLength(index, length));
                    transforms.put(variable, transform);
                }
                unpacker.readMapEnd();
            } else if ("vmeta".equals(entryName)) {
                int nSignals = unpacker.readMapBegin();
                for (int j = 0; j < nSignals; j++) {
                    String signal = unpacker.readString();
                    Map<String, Object> map = visitMetaMap(unpacker);
                    signalMeta.put(signal, map);
                }
                unpacker.readMapEnd();
            } else {
                throw new IOException("Unknown field " + entryName + " in defintion of table " + name);
            }
        }
        unpacker.readMapEnd();
        return new MeldTableReader(name, signals.toArray(new String[0]), offsets, transforms, tableMeta, signalMeta);
    }

    @Override
    protected final ReconObject visitObject(String name, Unpacker unpacker) throws IOException {
        Map<String, Object> objectMeta = null;
        int index = 0;
        int length = 0;
        int mapLength = unpacker.readMapBegin();
        for(int i=0;i<mapLength;i++) {
            String entryName = unpacker.readString();
            if("ometa".equals(entryName)) {
                objectMeta = visitMetaMap(unpacker);
            } else if("i".equals(entryName)) {
                index = unpacker.readInt();
            } else if("l".equals(entryName)) {
                length = unpacker.readInt();
            } else {
                throw new IOException("Unknown field " + entryName + " in defintion of table " + name);
            }
        }
        unpacker.readMapEnd();
        OffsetLength offsetLength = new OffsetLength(index, length);
        return new MeldObjectReader(name, objectMeta==null?new HashMap<String,Object>():objectMeta, offsetLength);
    }

    protected <T> T[] readSignal(Class<T> t, OffsetLength offsetLength) throws ReconException {
        try {
            byte[] bytes = new byte[offsetLength.getLength()];
            if (offsetLength.getLength() == resource.read(offsetLength.getOffset(), bytes)) {
                if(isCompressed()) {
                    bytes = Compression.decompress(bytes);
                }
                BufferUnpacker unpacker = getMessagePack().createBufferUnpacker(bytes);
                int arrayLength = unpacker.readArrayBegin();
                T[] out = ObjectArrays.newArray(t, arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    out[i] = (T)readObject(unpacker);
                }
                unpacker.readArrayEnd();
                unpacker.close();
                return out;
            }
            throw new ReconException("Failed to read signal at location");
        } catch (IOException ex) {
            throw new ReconException("Failed to read signal", ex);
        }
    }
    
    protected Map<String,Object> readObject(OffsetLength offsetLength) throws ReconException {
        try {
            byte[] bytes = new byte[offsetLength.getLength()];
            if (offsetLength.getLength() == resource.read(offsetLength.getOffset(), bytes)) {
                if(isCompressed()) {
                    bytes = Compression.decompress(bytes);
                }
                BufferUnpacker unpacker = getMessagePack().createBufferUnpacker(bytes);
                Map<String,Object> out = visitMetaMap(unpacker);
                unpacker.close();
                return out;
            }
            throw new ReconException("Failed to read object at location");
        } catch (IOException ex) {
            throw new ReconException("Failed to read object", ex);
        }
    }

    class MeldTableReader extends ReconTableReader {

        private final Map<String, OffsetLength> offsets;
        private final Map<String, String> transforms;

        MeldTableReader(String name, String[] signals, Map<String, OffsetLength> offsets, Map<String, String> transforms, Map<String, Object> meta, Map<String, Map<String, Object>> signalMeta) {
            super(name, signals, meta, signalMeta);
            this.offsets = offsets;
            this.transforms = transforms;
        }

        @Override
        public Alias[] getAliases() {
            return new Alias[0];
        }

        @Override
        public String[] getVariables() {
            return getSignals();
        }

        @Override
        public Object[] getSignal(String signal) throws ReconException {
            OffsetLength ol = offsets.get(signal);
            if (ol == null) {
                throw new ReconException("Signal " + signal + " not found");
            }
            return Transforms.applyArray(Object.class, readSignal(Object.class, ol), transforms.containsKey(signal) ? transforms.get(signal) : "");
        }

        @Override
        public <T> T[] getSignal(String signal, Class<T> c) throws ReconException {
            OffsetLength ol = offsets.get(signal);
            if (ol == null) {
                throw new ReconException("Signal " + signal + " not found");
            }
            return Transforms.applyArray(c, readSignal(c, ol), transforms.containsKey(signal) ? transforms.get(signal) : "");
        }
    }

    class MeldObjectReader extends ReconObjectReader {
        private final OffsetLength ol;

        public MeldObjectReader(String name, Map<String, Object> meta, OffsetLength ol) {            
            super(name, meta);
            this.ol = ol;
        }

        @Override
        public Map<String, Object> getFields() throws ReconException {
            return readObject(ol);
        }
    }
}
