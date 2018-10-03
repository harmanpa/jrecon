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
import com.github.harmanpa.jrecon.exceptions.TransposedException;
import com.github.harmanpa.jrecon.exceptions.WriteOnlyException;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is responsible for writing wall files.
 *
 * @author pete
 */
public class WallWriter extends ReconWriter {

    /**
     * This is a unique ID that every wall file starts with so it can be
     * identified/verified.
     */
    private static final String WALL_ID = "recon:wall:v01";

    public WallWriter(File file) {
        super(file);
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
        if (!defined) {
            bufferPacker.packMapHeader(3);
            // Write file meta
            bufferPacker.packString("fmeta");
            packMeta(bufferPacker, getFileMeta());
            // Write table definitions
            bufferPacker.packString("tabs");
            bufferPacker.packMapHeader(getTables().size());
            for (ReconTable table : getTables().values()) {
                bufferPacker.packString(table.getName());
                bufferPacker.packMapHeader(4);
                bufferPacker.packString("tmeta");
                packMeta(bufferPacker, table.getTableMeta());
                bufferPacker.packString("sigs");
                bufferPacker.packArrayHeader(table.getSignals().length);
                for (String signal : table.getSignals()) {
                    bufferPacker.packString(signal);
                }
                bufferPacker.packString("als");
                bufferPacker.packMapHeader(table.getAliases().length);
                for (Alias alias : table.getAliases()) {
                    bufferPacker.packString(alias.getAlias());
                    bufferPacker.packMapHeader(alias.getTransform().isEmpty() ? 1 : 2);
                    bufferPacker.packString("s");
                    bufferPacker.packString(alias.getOf());
                    if (!alias.getTransform().isEmpty()) {
                        bufferPacker.packString("t");
                        bufferPacker.packString(alias.getTransform());
                    }
                }
                bufferPacker.packString("vmeta");
                bufferPacker.packMapHeader(table.getSignals().length);
                for (String s : table.getSignals()) {
                    bufferPacker.packString(s);
                    packMeta(bufferPacker, table.getSignalMeta(s));
                }
            }
            // Write object definitions
            bufferPacker.packString("objs");
            bufferPacker.packMapHeader(getObjects().size());
            for (ReconObject object : getObjects().values()) {
                bufferPacker.packString(object.getName());
                packMeta(bufferPacker, object.getObjectMeta());
            }
            byte[] bytes = bufferPacker.toByteArray();
            int variableHeaderSize = bytes.length;
            // Buffer fixed header
            buffer.put(WALL_ID.getBytes());
            buffer.putInteger(variableHeaderSize);
            // Buffer variable header
            buffer.put(bytes);
            bufferPacker.clear();
            defined = true;
        }
    }

    @Override
    protected ReconTable createTable(String name, Iterable<String> signals) {
        return new WallTableWriter(name, signals);
    }

    @Override
    protected ReconObject createObject(String name) {
        return new WallObjectWriter(name);
    }

    class WallTableWriter extends ReconTableWriter {

        private final Set<Alias> aliases;

        public WallTableWriter(String name, Iterable<String> signals) {
            super(name, signals);
            this.aliases = Sets.newLinkedHashSet();
        }

        @Override
        public void addRow(Object... data) throws ReconException {
            checkFinalized();
            if (data.length != getSignals().length) {
                throw new ReconException("Number of data elements must match number of signals");
            }
            try {
                bufferPacker.packMapHeader(1);
                bufferPacker.packString(getName());
                bufferPacker.packValue(objectToValue(data));
                byte[] bytes = bufferPacker.toByteArray();
                buffer.putInteger(bytes.length);
                buffer.put(bytes);
                bufferPacker.clear();
            } catch (IOException ex) {
                throw new ReconException("Error writing new row", ex);
            }
        }

        @Override
        public final void addAlias(final String alias, String of, String transform) throws ReconException {
            checkNotFinalized();
            checkSignalExistence(of, true);
            checkSignalExistence(alias, false);
            if (!Sets.filter(aliases, new Predicate<Alias>() {
                @Override
                public boolean apply(Alias t) {
                    return t.getAlias().equals(alias);
                }
            }).isEmpty()) {
                throw new ReconException("Alias already exists");
            }
            Alias a = new Alias(alias, of, transform);
            aliases.add(a);
        }

        @Override
        public final Alias[] getAliases() {
            return Iterables.toArray(aliases, Alias.class);
        }

        @Override
        public final String[] getVariables() {
            List<String> vars = Lists.newArrayList();
            for (Alias alias : aliases) {
                vars.add(alias.getAlias());
            }
            vars.addAll(signals);
            return vars.toArray(new String[0]);
        }

        @Override
        public void setSignal(String signal, Object... data) throws ReconException {
            throw new TransposedException();
        }
    }

    /**
     * This class is used to write object fields back to a wall.
     */
    class WallObjectWriter extends ReconObjectWriter {

        public WallObjectWriter(String name) {
            super(name);
        }

        @Override
        public final void addField(String name, Object value) throws ReconException {
            checkFinalized();
            try {
                bufferPacker.packMapHeader(1);
                bufferPacker.packString(getName());
                bufferPacker.packMapHeader(1);
                bufferPacker.packString(name);
                bufferPacker.packValue(objectToValue(value));
                byte[] bytes = bufferPacker.toByteArray();
                buffer.putInteger(bytes.length);
                buffer.put(bytes);
                bufferPacker.clear();
            } catch (IOException ex) {
                throw new ReconException("Error writing new field", ex);
            }
        }

        @Override
        public final Map<String, Object> getFields() throws ReconException {
            throw new WriteOnlyException();
        }
    }
}
