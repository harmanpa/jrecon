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

import java.io.Serializable;

/**
 *
 * @author pete
 */
public class Alias implements Serializable {
    private final String alias;
    private final String of;
    private final String transform;

    public Alias(String alias, String of, String transform) {
        this.alias = alias;
        this.of = of;
        this.transform = transform;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 71 * hash + (this.alias != null ? this.alias.hashCode() : 0);
        hash = 71 * hash + (this.of != null ? this.of.hashCode() : 0);
        hash = 71 * hash + (this.transform != null ? this.transform.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Alias other = (Alias) obj;
        if ((this.alias == null) ? (other.alias != null) : !this.alias.equals(other.alias)) {
            return false;
        }
        if ((this.of == null) ? (other.of != null) : !this.of.equals(other.of)) {
            return false;
        }
        return !((this.transform == null) ? (other.transform != null) : !this.transform.equals(other.transform));
    }

    @Override
    public String toString() {
        return "Alias{" + "alias=" + alias + ", of=" + of + ", transform=" + transform + '}';
    }

    public String getAlias() {
        return alias;
    }

    public String getOf() {
        return of;
    }

    public String getTransform() {
        return transform;
    }
    
}
