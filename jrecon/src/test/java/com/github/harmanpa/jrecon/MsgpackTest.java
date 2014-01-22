/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.harmanpa.jrecon;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.template.Template;
import org.msgpack.unpacker.Unpacker;

/**
 *
 * @author pete
 */
public class MsgpackTest {
    
    @Test
    public void testMap() {
        
    }

    @Test
    public void test() throws IOException {
        Object[] objs = new Object[]{1.0,2.0,3.0};
        double[] dubs = new double[]{1.0,2.0,3.0};
        MessagePack pack = new MessagePack();
        pack.register(Object[].class, new Template<Object[]>() {

            public void write(Packer packer, Object[] t) throws IOException {
                packer.writeArrayBegin(t.length);
                for(Object o : t) {
                    packer.write(o);
                }
                packer.writeArrayEnd();
            }

            public void write(Packer packer, Object[] t, boolean bln) throws IOException {
                packer.writeArrayBegin(t.length);
                for(Object o : t) {
                    packer.write(o);
                }
                packer.writeArrayEnd();
            }

            public Object[] read(Unpacker unpckr, Object[] t) throws IOException {
                int n = unpckr.readArrayBegin();
                Object[] out = new Object[n];
                for(int i=0;i<n;i++) {
                    out[i] = unpckr.read(Object.class);
                }
                unpckr.readArrayEnd();
                return out;
            }

            public Object[] read(Unpacker unpckr, Object[] t, boolean bln) throws IOException {
                int n = unpckr.readArrayBegin();
                Object[] out = new Object[n];
                for(int i=0;i<n;i++) {
                    out[i] = unpckr.read(Object.class);
                }
                unpckr.readArrayEnd();
                return out;
            }
        });
        Assert.assertArrayEquals(pack.write(objs), pack.write(dubs));
    }
}
