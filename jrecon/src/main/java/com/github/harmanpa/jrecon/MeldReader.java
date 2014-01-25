package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.ReconException;
import com.github.harmanpa.jrecon.io.RandomAccessResource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    protected final String getFileTypeString() {
        return "recon:meld:v01";
    }

    protected final byte[] readFixedHeaderBytes() throws IOException {
        byte[] fixed = new byte[18];
        if (18 != this.resource.read(0L, fixed)) {
            throw new IOException("Could not read fixed header");
        }
        return fixed;
    }

    protected final byte[] readVariableHeaderBytes(int size) throws IOException {
        byte[] variableHeaderBytes = new byte[size];
        if (size != this.resource.read(18L, variableHeaderBytes)) {
            throw new IOException("Could not read variable header");
        }
        return variableHeaderBytes;
    }

    protected final ReconTable visitTable(String name, Unpacker unpacker) throws IOException {
        Map<String, Object> tableMeta = new HashMap<String, Object>();
        Map<String, Map<String, Object>> signalMeta = new HashMap<String, Map<String, Object>>();
        List<String> signals = new ArrayList<String>();
        List<Alias> aliases = new ArrayList<Alias>();
        int mapLength = unpacker.readMapBegin();
        for (int i = 0; i < mapLength; i++) {
            String entryName = unpacker.readString();
            if ("tmeta".equals(entryName)) {
                tableMeta.putAll(visitMetaMap(unpacker));
            } else if ("sigs".equals(entryName)) {
                int nSignals = unpacker.readArrayBegin();
                for (int j = 0; j < nSignals; j++) {
                    signals.add(unpacker.readString());
                }
                unpacker.readArrayEnd();
            } else if ("als".equals(entryName)) {
                int nAliases = unpacker.readMapBegin();
                for (int j = 0; j < nAliases; j++) {
                    String alias = unpacker.readString();
                    String of = "";
                    String transform = "";
                    int nData = unpacker.readMapBegin();
                    for (int k = 0; k < nData; k++) {
                        String aliasData = unpacker.readString();
                        if ("s".equals(aliasData)) {
                            of = unpacker.readString();
                        } else if ("t".equals(aliasData)) {
                            transform = unpacker.readString();
                        }
                    }
                    unpacker.readMapEnd();
                    aliases.add(new Alias(alias, of, transform));
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
            }
        }
        unpacker.readMapEnd();
        return new MeldTableReader(name, signals.toArray(new String[0]), aliases.toArray(new Alias[0]), tableMeta, signalMeta);
    }

    protected final ReconObject visitObject(String name, Unpacker unpacker) throws IOException {
        Map<String, Object> objectMeta = visitMetaMap(unpacker);
        return new MeldObjectReader(name, objectMeta);
    }

    class MeldTableReader extends ReconTableReader {

        public MeldTableReader(String name, String[] signals, Alias[] aliases, Map<String, Object> meta, Map<String, Map<String, Object>> signalMeta) {
            super(name, signals, aliases, meta, signalMeta);
        }

        public Object[] getSignal(String signal) throws ReconException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        public <T> T[] getSignal(String signal, Class<T> c) throws ReconException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }

    class MeldObjectReader extends ReconObjectReader {

        public MeldObjectReader(String name, Map<String, Object> meta) {
            super(name, meta);
        }

        public Map<String, Object> getFields() throws ReconException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }
}
