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
import com.github.harmanpa.jrecon.exceptions.NotFinalizedException;
import com.github.harmanpa.jrecon.exceptions.ReconException;
import com.github.harmanpa.jrecon.exceptions.WriteOnlyException;
import com.github.harmanpa.jrecon.utils.ExpandableByteBuffer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

/**
 *
 * @author pete
 */
public abstract class ReconWriter extends ReconFile {

    protected final File file;
    protected boolean defined;
    private final Map<String, ReconTable> tables;
    private final Map<String, ReconObject> objects;
    private final Map<String, Object> meta;
    protected final ExpandableByteBuffer buffer;
    protected final MessageBufferPacker bufferPacker;

    public ReconWriter(File file) {
        this.file = file;
        this.defined = false;
        this.tables = Maps.newHashMap();
        this.objects = Maps.newHashMap();
        this.meta = Maps.newHashMap();
        this.buffer = new ExpandableByteBuffer(ByteBuffer.allocate(8 * 1024 * 1024).order(ByteOrder.BIG_ENDIAN));
        this.bufferPacker = MessagePack.newDefaultBufferPacker();
    }

    /**
     *
     * @return
     */
    @Override
    public final boolean isDefined() {
        return defined;
    }

    /**
     * This checks any new name introduced (for either a table or an object) to
     * make sure it is unique across the wall.
     *
     * @param name
     * @throws ReconException
     */
    protected final void checkName(String name) throws ReconException {
        if (tables.containsKey(name) || objects.containsKey(name)) {
            throw new ReconException(name + " already exists");
        }
    }

    /**
     *
     * @throws ReconException
     */
    protected final void checkNotFinalized() throws ReconException {
        if (defined) {
            throw new FinalizedException();
        }
    }

    /**
     *
     * @throws ReconException
     */
    protected final void checkFinalized() throws ReconException {
        if (!defined) {
            throw new NotFinalizedException();
        }
    }

    /**
     * This adds a new table to the wall. If the wall has been finalized, this
     * will generated a FinalizedWall exception. If the name is already used by
     * either a table or object, an exception will be raised. Otherwise, a
     * WallTableWriter object will be returned by this method that can be used
     * to populate the table.
     *
     * @param name
     * @param signals
     * @return
     * @throws ReconException
     */
    @Override
    public final ReconTable addTable(String name, String... signals) throws ReconException {
        checkNotFinalized();
        checkName(name);
        ReconTable table = createTable(name, Arrays.asList(signals));
        tables.put(name, table);
        return table;
    }

    /**
     * This adds a new object to the wall. If the wall has been finalized, this
     * will generated a FinalizedWall exception. If the name is already used by
     * either a table or object, an exception will be raised. Otherwise, a
     * WallObjectWriter object will be returned by this method that can be used
     * to populate the fields of the object.
     *
     * @param name
     * @return
     * @throws ReconException
     */
    @Override
    public final ReconObject addObject(String name) throws ReconException {
        checkNotFinalized();
        checkName(name);
        ReconObject object = createObject(name);
        objects.put(name, object);
        return object;
    }

    @Override
    public final void addMeta(String name, Object value) throws ReconException {
        checkNotFinalized();
        meta.put(name, value);
    }

    @Override
    public final Map<String, Object> getFileMeta() {
        return ImmutableMap.copyOf(meta);
    }

    @Override
    public final Map<String, ReconTable> getTables() {
        return ImmutableMap.copyOf(tables);
    }

    @Override
    public final Map<String, ReconObject> getObjects() {
        return ImmutableMap.copyOf(objects);
    }

    protected abstract ReconTable createTable(String name, Iterable<String> signals);

    protected abstract ReconObject createObject(String name);

    /**
     * This flushes any pending rows of fields.
     *
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
        try (FileChannel channel = new FileOutputStream(file, defined).getChannel()) {
            buffer.writeToChannel(channel);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    protected void packMeta(MessageBufferPacker packer, Map<String, Object> meta) throws IOException {
        packer.packMapHeader(meta.size());
        for (Map.Entry<String, Object> entry : meta.entrySet()) {
            packer.packString(entry.getKey());
            packer.packValue(objectToValue(entry.getValue()));
        }
    }

    static final Map<Class<?>, Function<Object, Value>> VALUECONVERTERS = Maps.newHashMap();

    static {
        VALUECONVERTERS.put(double.class, (v) -> ValueFactory.newFloat((double) v));
        VALUECONVERTERS.put(float.class, (v) -> ValueFactory.newFloat((float) v));
        VALUECONVERTERS.put(int.class, (v) -> ValueFactory.newInteger((int) v));
        VALUECONVERTERS.put(long.class, (v) -> ValueFactory.newInteger((long) v));
        VALUECONVERTERS.put(short.class, (v) -> ValueFactory.newInteger((short) v));
        VALUECONVERTERS.put(byte.class, (v) -> ValueFactory.newInteger((byte) v));
        VALUECONVERTERS.put(Double.class, (v) -> ValueFactory.newFloat((Double) v));
        VALUECONVERTERS.put(Float.class, (v) -> ValueFactory.newFloat((Float) v));
        VALUECONVERTERS.put(Integer.class, (v) -> ValueFactory.newInteger((Integer) v));
        VALUECONVERTERS.put(Long.class, (v) -> ValueFactory.newInteger((Long) v));
        VALUECONVERTERS.put(Short.class, (v) -> ValueFactory.newInteger((Short) v));
        VALUECONVERTERS.put(Byte.class, (v) -> ValueFactory.newInteger((Byte) v));
        VALUECONVERTERS.put(BigInteger.class, (v) -> ValueFactory.newInteger((BigInteger) v));
        VALUECONVERTERS.put(String.class, (v) -> ValueFactory.newString((String) v));
        VALUECONVERTERS.put(Map.class, (v) -> {
            Map<Object, Object> map = (Map<Object, Object>) v;
            List<Map.Entry<Value, Value>> entries = new ArrayList<>(map.size());
            map.entrySet().forEach((entry) -> {
                try {
                    entries.add(ValueFactory.newMapEntry(objectToValue(entry.getKey()), objectToValue(entry.getValue())));
                } catch (IOException ex) {
                }
            });
            return ValueFactory.newMap(entries.toArray(new Map.Entry[0]));
        });
    }

    protected static Value objectToValue(Object object) throws IOException {
        Class<?> objectType = object.getClass();
        if (objectType.isArray()) {
            int n = Array.getLength(object);
            if (n == 0) {
                return ValueFactory.emptyArray();
            }
            Function<Object, Value> conv = VALUECONVERTERS.get(objectType.getComponentType());
            if (conv == null) {
                conv = VALUECONVERTERS.get(Array.get(object, 0).getClass());
                if (conv == null) {
                    throw new IOException("Unsupported type in serialization: " + objectType.getComponentType());
                }
            }
            Value[] values = new Value[n];
            for (int i = 0; i < values.length; i++) {
                values[i] = conv.apply(Array.get(object, i));
            }
            return ValueFactory.newArray(values);
        } else {
            Function<Object, Value> conv = VALUECONVERTERS.get(objectType);
            if (conv == null) {
                throw new IOException("Unsupported type in serialization: " + objectType);
            }
            return conv.apply(object);
        }
    }

    abstract class ReconTableWriter implements ReconTable {

        private final String name;
        protected final Set<String> signals;
        private final Map<String, Object> meta;
        private final Map<String, Map<String, Object>> signalMeta;

        ReconTableWriter(String name, Iterable<String> signals) {
            this.name = name;
            this.signals = Sets.newLinkedHashSet(signals);
            this.meta = Maps.newHashMap();
            this.signalMeta = Maps.newHashMap();
        }

        protected void checkSignalExistence(String signal, boolean shouldExist) throws ReconException {
            if (shouldExist && !signals.contains(signal)) {
                throw new ReconException("Signal " + signal + " does not exist");
            }
            if (!shouldExist && signals.contains(signal)) {
                throw new ReconException("Signal " + signal + " already exists");
            }
        }

        @Override
        public void addSignal(String signal) throws ReconException {
            checkNotFinalized();
            checkSignalExistence(signal, false);
            signals.add(signal);
        }

        @Override
        public final void addAlias(String alias, String of) throws ReconException {
            addAlias(alias, of, "");
        }

        @Override
        public final String getName() {
            return name;
        }

        @Override
        public final String[] getSignals() {
            return signals.toArray(new String[0]);
        }

        @Override
        public final Map<String, Object> getTableMeta() {
            return ImmutableMap.copyOf(meta);
        }

        @Override
        public final Map<String, Object> getSignalMeta(String signal) {
            if (signalMeta.containsKey(signal)) {
                return ImmutableMap.copyOf(signalMeta.get(signal));
            }
            return ImmutableMap.of();
        }

        @Override
        public final void addMeta(String name, Object data) throws ReconException {
            checkNotFinalized();
            meta.put(name, data);
        }

        @Override
        public final void addSignalMeta(String signal, String name, Object data) throws ReconException {
            checkNotFinalized();
            checkSignalExistence(signal, true);
            if (!signalMeta.containsKey(signal)) {
                signalMeta.put(signal, new HashMap<>());
            }
            signalMeta.get(signal).put(name, data);
        }

        @Override
        public final Object[] getSignal(String signal) throws ReconException {
            throw new WriteOnlyException();
        }

        @Override
        public final <T> T[] getSignal(String signal, Class<T> c) throws ReconException {
            throw new WriteOnlyException();
        }
    }

    abstract class ReconObjectWriter implements ReconObject {

        private final String name;
        private final Map<String, Object> meta;

        ReconObjectWriter(String name) {
            this.name = name;
            this.meta = Maps.newHashMap();
        }

        @Override
        public final String getName() {
            return name;
        }

        @Override
        public final Map<String, Object> getObjectMeta() {
            return ImmutableMap.copyOf(meta);
        }

        @Override
        public final void addMeta(String name, Object value) throws ReconException {
            checkNotFinalized();
            meta.put(name, value);
        }

    }
}
