package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.FinalizedException;
import com.github.harmanpa.jrecon.exceptions.NotFinalizedException;
import com.github.harmanpa.jrecon.exceptions.ReconException;
import com.github.harmanpa.jrecon.exceptions.WriteOnlyException;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;

/**
 *
 * @author pete
 */
public abstract class ReconWriter implements ReconFile {

    protected final File file;
    protected boolean defined;
    private final Map<String, ReconTable> tables;
    private final Map<String, ReconObject> objects;
    private final Map<String, Object> meta;
    protected final ByteBuffer buffer;
    protected final BufferPacker bufferPacker;

    public ReconWriter(File file) {
        this.file = file;
        this.defined = false;
        this.tables = Maps.newHashMap();
        this.objects = Maps.newHashMap();
        this.meta = Maps.newHashMap();
        this.buffer = ByteBuffer.allocate(8 * 1024 * 1024).order(ByteOrder.BIG_ENDIAN);
        this.bufferPacker = new MessagePack().createBufferPacker();
    }

    /**
     *
     * @return
     */
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
    public final ReconTable addTable(String name, String... signals) throws ReconException {
        checkNotFinalized();
        checkName(name);
        ReconTable table = createTable(name, signals);
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
    public final ReconObject addObject(String name) throws ReconException {
        checkNotFinalized();
        checkName(name);
        ReconObject object = createObject(name);
        objects.put(name, object);
        return object;
    }

    public final void addMeta(String name, Object value) throws ReconException {
        checkNotFinalized();
        meta.put(name, value);
    }

    public final Map<String, Object> getFileMeta() {
        return ImmutableMap.copyOf(meta);
    }

    public final Map<String, ReconTable> getTables() {
        return ImmutableMap.copyOf(tables);
    }

    public final Map<String, ReconObject> getObjects() {
        return ImmutableMap.copyOf(objects);
    }
    protected abstract ReconTable createTable(String name, String[] signals);
    
    protected abstract ReconObject createObject(String name);

    abstract class ReconTableWriter implements ReconTable {

        private final String name;
        private final String[] signals;
        private final Set<Alias> aliases;
        private final Map<String, Object> meta;
        private final Map<String, Map<String, Object>> signalMeta;

        ReconTableWriter(String name, String[] signals) {
            this.name = name;
            this.signals = signals;
            this.aliases = Sets.newLinkedHashSet();
            this.meta = Maps.newHashMap();
            this.signalMeta = Maps.newHashMap();
        }

        public final void addAlias(String alias, String of) throws ReconException {
            addAlias(alias, of, "");
        }

        public final void addAlias(final String alias, String of, String transform) throws ReconException {
            checkNotFinalized();
            if (!Arrays.asList(signals).contains(of)) {
                throw new ReconException("Attempting to alias non-existent variable");
            }
            if (!Sets.filter(aliases, new Predicate<Alias>() {

                public boolean apply(Alias t) {
                    return t.getAlias().equals(alias);
                }
            }).isEmpty()) {
                throw new ReconException("Alias already exists");
            }
            Alias a = new Alias(alias, of, transform);
            aliases.add(a);
        }

        public final String getName() {
            return name;
        }

        public final String[] getSignals() {
            return signals;
        }

        public final Alias[] getAliases() {
            return Iterables.toArray(aliases, Alias.class);
        }

        public final Map<String, Object> getTableMeta() {
            return ImmutableMap.copyOf(meta);
        }

        public final Map<String, Object> getSignalMeta(String signal) {
            if (signalMeta.containsKey(signal)) {
                return ImmutableMap.copyOf(signalMeta.get(signal));
            }
            return ImmutableMap.of();
        }

        public final void addMeta(String name, Object data) throws ReconException {
            checkNotFinalized();
            meta.put(name, data);
        }

        public final void addSignalMeta(String signal, String name, Object data) throws ReconException {
            checkNotFinalized();
            if (!signalMeta.containsKey(signal)) {
                signalMeta.put(signal, new HashMap<String, Object>());
            }
            signalMeta.get(signal).put(name, data);
        }

        public final Object[] getSignal(String signal) throws ReconException {
            throw new WriteOnlyException();
        }

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

        public final String getName() {
            return name;
        }

        public final Map<String, Object> getObjectMeta() {
            return ImmutableMap.copyOf(meta);
        }

        public final void addMeta(String name, Object value) throws ReconException {
            checkNotFinalized();
            meta.put(name, value);
        }

    }
}
