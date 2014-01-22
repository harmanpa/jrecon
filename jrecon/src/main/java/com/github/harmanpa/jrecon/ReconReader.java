package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.FinalizedException;
import com.github.harmanpa.jrecon.exceptions.ReadOnlyException;
import com.github.harmanpa.jrecon.exceptions.ReconException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author pete
 */
public abstract class ReconReader implements ReconFile {

    public ReconTable addTable(String name, String... signals) throws ReconException {
        throw new FinalizedException();
    }

    public ReconObject addObject(String name) throws ReconException {
        throw new FinalizedException();
    }

    public void addMeta(String name, Object value) throws ReconException {
        throw new FinalizedException();
    }

    public boolean isDefined() {
        return true;
    }

    public void finalizeDefinitions() throws IOException {
    }

    abstract class ReconObjectReader implements ReconObject {

        private final String name;
        private final Map<String, Object> meta;

        public ReconObjectReader(String name, Map<String, Object> meta) {
            this.name = name;
            this.meta = meta;
        }

        public final String getName() {
            return name;
        }

        public final Map<String, Object> getObjectMeta() {
            return meta;
        }

        public final void addField(String name, Object value) throws ReconException {
            throw new ReadOnlyException();
        }

        public final void addMeta(String name, Object value) throws ReconException {
            throw new FinalizedException();
        }

    }

    abstract class ReconTableReader implements ReconTable {

        private final String name;
        private final String[] signals;
        private final Alias[] aliases;
        private final Map<String, Object> meta;
        private final Map<String, Map<String, Object>> signalMeta;

        public ReconTableReader(String name, String[] signals, Alias[] aliases, Map<String, Object> meta, Map<String, Map<String, Object>> signalMeta) {
            this.name = name;
            this.signals = signals;
            this.aliases = aliases;
            this.meta = meta;
            this.signalMeta = signalMeta;
        }

        public final String getName() {
            return name;
        }

        public final String[] getSignals() {
            return signals;
        }

        public final Alias[] getAliases() {
            return aliases;
        }

        public final Map<String, Object> getTableMeta() {
            return meta;
        }

        public final Map<String, Object> getSignalMeta(String signal) {
            return signalMeta.containsKey(signal) ? signalMeta.get(signal) : new HashMap<String, Object>();
        }

        public final void addRow(Object... data) throws ReconException {
            throw new ReadOnlyException();
        }

        public final void addMeta(String name, Object data) throws ReconException {
            throw new FinalizedException();
        }

        public final void addSignalMeta(String signal, String name, Object data) throws ReconException {
            throw new FinalizedException();
        }

        public final void addAlias(String var, String alias, String transform) throws ReconException {
            throw new FinalizedException();
        }

        public final void addAlias(String var, String alias) throws ReconException {
            throw new FinalizedException();
        }

        public final void setSignal(String signal, Object... data) throws ReconException {
            throw new ReadOnlyException();
        }


    }
}
