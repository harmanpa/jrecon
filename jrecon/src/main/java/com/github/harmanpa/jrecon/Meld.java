
package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.ReconException;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author pete
 */
public class Meld {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if(args.length==2) {
            try {
                wallToMeld(new File(args[0]), new File(args[1]));
            } catch (IOException ex) {
                Logger.getLogger(Meld.class.getName()).log(Level.SEVERE, null, ex);
            } catch(ReconException ex) {
                Logger.getLogger(Meld.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public static void wallToMeld(File wall, File meld) throws IOException, ReconException {
        WallReader reader = new WallReader(wall);
        MeldWriter writer = new MeldWriter(meld);
        // Add meta data
        for(Map.Entry<String,Object> entry : reader.getFileMeta().entrySet()) {
            writer.addMeta(entry.getKey(), entry.getValue());
        }
        // Iterate over tables, adding each table, it's signals and aliases
        for(ReconTable table : reader.getTables().values()) {
            ReconTable newTable = writer.addTable(table.getName(), table.getSignals());
            for(Alias alias : table.getAliases()) {
                newTable.addAlias(alias.getAlias(), alias.getOf(), alias.getTransform());
            }
            for(Map.Entry<String,Object> entry : table.getTableMeta().entrySet()) {
                newTable.addMeta(entry.getKey(), entry.getValue());
            }
            for(String signal : table.getSignals()) {
                for(Map.Entry<String,Object> entry : table.getSignalMeta(signal).entrySet()) {
                    newTable.addSignalMeta(signal, entry.getKey(), entry.getValue());
                }
            }
        }
        // Iterate over objects
        for(ReconObject object : reader.getObjects().values()) {
            ReconObject newObject = writer.addObject(object.getName());
            for(Map.Entry<String,Object> entry : object.getObjectMeta().entrySet()) {
                newObject.addMeta(entry.getKey(), entry.getValue());
            }
            for(Map.Entry<String,Object> entry : object.getFields().entrySet()) {
                newObject.addField(entry.getKey(), entry.getValue());
            }
        }
        // Iterate over rows, building table data
        // Write
    }
}
