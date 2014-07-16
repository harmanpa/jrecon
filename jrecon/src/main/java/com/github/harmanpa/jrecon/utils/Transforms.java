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
package com.github.harmanpa.jrecon.utils;

import com.github.harmanpa.jrecon.exceptions.ReconException;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ObjectArrays;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;

/**
 *
 * @author pete
 */
public class Transforms {

    public static <T> T[] applyArray(Class<T> t, T[] obj, String transform) throws ReconException {
        if ("inv".equals(transform)) {
            T[] out = ObjectArrays.newArray(t, obj.length);
            for (int i = 0; i < obj.length; i++) {
                out[i] = inverse(obj[i]);
            }
            return out;
        }
        if (transform.startsWith("aff")) {
            Number[] args = parseAffine(transform);
            T[] out = ObjectArrays.newArray(t, obj.length);
            for (int i = 0; i < obj.length; i++) {
                out[i] = affine(obj[i], args[0], args[1]);
            }
            return out;
        }
        if ("".equals(transform)) {
            return obj;
        }
        throw new ReconException("Unsupported transform: " + transform);
    }

    public static <T> T apply(Class<T> t, T obj, String transform) throws ReconException {
        if ("inv".equals(transform)) {
            return inverse(obj);
        }
        if (transform.startsWith("aff")) {
            Number[] args = parseAffine(transform);
            return affine(obj, args[0], args[1]);
        }
        if ("".equals(transform)) {
            return obj;
        }
        throw new ReconException("Unsupported transform: " + transform);
    }

    private static <T> T inverse(T obj) throws ReconException {
        if (obj instanceof Double) {
            return (T) Double.valueOf(-1.0 * ((Double) obj).doubleValue());
        }
        if (obj instanceof Integer) {
            return (T) Integer.valueOf(-1 * ((Integer) obj).intValue());
        }
        if (obj instanceof Boolean) {
            return (T) Boolean.valueOf(!((Boolean) obj).booleanValue());
        }
        throw new ReconException("Cannot invert " + obj);
    }

    private static <T> T affine(T obj, Number a, Number b) throws ReconException {
        if (obj instanceof Double) {
            return (T) Double.valueOf(b.doubleValue() + a.doubleValue() * ((Double) obj).doubleValue());
        }
        if (obj instanceof Integer) {
            return (T) Integer.valueOf(b.intValue() + a.intValue() * ((Integer) obj).intValue());
        }
        throw new ReconException("Cannot apply affine transform to " + obj);
    }

    private static Number[] parseAffine(String transform) throws ReconException {
        List<String> strs = Splitter.on(CharMatcher.anyOf("(,)")).omitEmptyStrings().splitToList(transform);
        Number[] out = new Number[2];
        if (strs.size() == 3) {
            try {
                out[0] = NumberFormat.getInstance().parse(strs.get(1));
                out[1] = NumberFormat.getInstance().parse(strs.get(2));
            } catch (ParseException ex) {
                throw new ReconException("Could not parse " + transform, ex);
            }
            return out;
        }
        throw new ReconException("Could not parse " + transform);
    }
}
