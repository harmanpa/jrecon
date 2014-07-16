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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.packer.Packer;
import org.msgpack.template.Template;
import org.msgpack.type.Value;
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
    
    @Test
    public void test2() {
        MessagePack pack = new MessagePack();
        
    }
}
