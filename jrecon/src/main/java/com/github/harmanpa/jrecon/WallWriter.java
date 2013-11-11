package com.github.harmanpa.jrecon;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.bson.BSONEncoder;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.BasicBSONList;

/**
 *
 * @author pete
 */
public class WallWriter {

    private static final String WALL_ID = "recon:wall";

    private final File file;
    private final boolean verbose;
    private boolean defined;
    private final Map<String, WallTableWriter> tables;
    private final Map<String, WallObjectWriter> objects;
    private final BSONEncoder encoder;
    private final OutputBuffer buffer;

    public WallWriter(File file, boolean verbose) {
        this.file = file;
        this.verbose = verbose;
        this.defined = false;
        this.tables = Maps.newHashMap();
        this.objects = Maps.newHashMap();
        this.encoder = new BasicBSONEncoder();
        this.buffer = new BasicOutputBuffer();
        encoder.set(buffer);
    }

    public WallTableWriter addTable(String name, String... signals) {
        WallTableWriter table = new WallTableWriter(name, signals);
        return table;
    }

    public WallObjectWriter addObject(String name) {
        WallObjectWriter object = new WallObjectWriter(name);
        return object;
    }

    public final void flush() throws IOException {
        FileOutputStream os = new FileOutputStream(file);
        buffer.pipe(os);
        buffer.setPosition(0);
        os.close();
    }

    public final boolean isVerbose() {
        return verbose;
    }

    public final boolean isDefined() {
        return defined;
    }

    public final void finalizeDefinitions() {
        if (!defined) {

            defined = true;
        }
    }

    public class WallTableWriter {

        private final String name;
        private final String[] signals;

        WallTableWriter(String name, String[] signals) {
            this.name = name;
            this.signals = signals;
        }

        public void addRow(Object... data) {
            BasicBSONList list = new BasicBSONList();
            list.addAll(Arrays.asList(data));
            encoder.putObject(new BasicBSONObject(name, list));
        }
    }

    public class WallObjectWriter {

        private final String name;

        WallObjectWriter(String name) {
            this.name = name;
        }

    }
}
