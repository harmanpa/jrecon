package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.FinalizedException;
import com.github.harmanpa.jrecon.exceptions.NotFinalizedException;
import com.github.harmanpa.jrecon.exceptions.WallException;
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
 * This class is responsible for writing wall files.
 *
 * @author pete
 */
public class WallWriter {

    /**
     * This is a unique ID that every wall file starts with so it can be
     * identified/verified.
     */
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
     * @throws WallException
     */
    public WallTableWriter addTable(String name, String... signals) throws WallException {
        checkNotFinalized();
        checkName(name);
        WallTableWriter table = new WallTableWriter(name, signals);
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
     * @throws WallException
     */
    public WallObjectWriter addObject(String name) throws WallException {
        checkNotFinalized();
        checkName(name);
        WallObjectWriter object = new WallObjectWriter(name);
        objects.put(name, object);
        return object;
    }

    /**
     * This flushes any pending rows of fields.
     *
     * @throws IOException
     */
    public final void flush() throws IOException {
        FileOutputStream os = new FileOutputStream(file);
        buffer.pipe(os);
        buffer.setPosition(0);
        os.close();
    }

    /**
     *
     * @return
     */
    public final boolean isVerbose() {
        return verbose;
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
     * @throws WallException
     */
    private void checkName(String name) throws WallException {
        if (tables.containsKey(name) || objects.containsKey(name)) {
            throw new WallException(name + " already exists");
        }
    }

    /**
     *
     * @throws WallException
     */
    private void checkNotFinalized() throws WallException {
        if (defined) {
            throw new FinalizedException();
        }
    }

    /**
     *
     * @throws WallException
     */
    private void checkFinalized() throws WallException {
        if (!defined) {
            throw new NotFinalizedException();
        }
    }

    /**
     * This method is called when all tables and objects have been defined. Once
     * called, it is not possible to add new tables or objects. Furthermore, it
     * is not possible to add rows or fields until the wall has been finalized.
     */
    public final void finalizeDefinitions() {
        if (!defined) {

            defined = true;
        }
    }

    /**
     * This class is used to add rows to a given wall.
     */
    public class WallTableWriter {

        private final String name;
        private final String[] signals;

        WallTableWriter(String name, String[] signals) {
            this.name = name;
            this.signals = signals;
        }

        /**
         * Defines an alias associated with a specific table.
         * The value of the alias variable will be computed by
         * multiplying the base variable by the scale factor and then adding the
         * offset value.
         * 
         * @param alias The name of the alias
         * @param of The variable it is an alias of (cannot also be an alias)
         * @throws WallException 
         */
        public void addAlias(String alias, String of) throws WallException {
            addAlias(alias, of, 1.0);
        }

        /**
         * Defines an alias associated with a specific table.
         * The value of the alias variable will be computed by
         * multiplying the base variable by the scale factor and then adding the
         * offset value.
         * 
         * @param alias The name of the alias
         * @param of The variable it is an alias of (cannot also be an alias)
         * @param scale The scale factor
         * @throws WallException 
         */
        public void addAlias(String alias, String of, double scale) throws WallException {
            addAlias(alias, of, scale, 0.0);
        }

        /**
         * Defines an alias associated with a specific table.
         * The value of the alias variable will be computed by
         * multiplying the base variable by the scale factor and then adding the
         * offset value.
         * 
         * @param alias The name of the alias
         * @param of The variable it is an alias of (cannot also be an alias)
         * @param scale The scale factor
         * @param offset The offset value between the alias and base variable
         * @throws WallException 
         */
        public void addAlias(String alias, String of, double scale, double offset) throws WallException {
            checkNotFinalized();
            // TODO: Add the alias
        }

        /**
         * Adds a row to the table
         * @param data 
         */
        public void addRow(Object... data) {
            BasicBSONList list = new BasicBSONList();
            list.addAll(Arrays.asList(data));
            encoder.putObject(new BasicBSONObject(name, list));
        }
    }

    /**
     * This class is used to write object fields back to a wall.
     */
    public class WallObjectWriter {

        private final String name;

        WallObjectWriter(String name) {
            this.name = name;
        }
        
        public void addField(String name, Object value) {
            
        }

    }
}
