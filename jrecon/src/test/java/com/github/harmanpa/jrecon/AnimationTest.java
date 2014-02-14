package com.github.harmanpa.jrecon;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pete
 */
public class AnimationTest {

    @Test
    public void test0() {
        File f = new File(new File(System.getProperty("user.dir")), "src/test/resources/samples/FB13Jan.wll");
        try {
            WallReader reader = new WallReader(f);
            for (ReconObject obj : reader.getObjects().values()) {
                for (Map.Entry<String, Object> entry : obj.getFields().entrySet()) {
                    Object shapeObject = entry.getValue();
                    if(shapeObject instanceof Map) {
                        System.out.println("Shape: " + entry.getKey());
                        Map<Object,Object> shapeMap = (Map<Object,Object>)shapeObject;
                        for(Map.Entry<Object,Object> shapeField : shapeMap.entrySet()) {                            
                            System.out.println("\t" + shapeField.getKey() + "=" + (shapeField.getValue().getClass().isArray()?Arrays.toString((Object[])shapeField.getValue()):shapeField.getValue()));
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
}
