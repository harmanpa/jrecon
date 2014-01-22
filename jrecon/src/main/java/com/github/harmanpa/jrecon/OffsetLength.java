/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.github.harmanpa.jrecon;

/**
 *
 * @author pete
 */
public class OffsetLength {
    private final int offset;
    private final int length;

    public OffsetLength(int offset, int length) {
        this.offset = offset;
        this.length = length;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + this.offset;
        hash = 37 * hash + this.length;
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
        final OffsetLength other = (OffsetLength) obj;
        if (this.offset != other.offset) {
            return false;
        }
        if (this.length != other.length) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "OffsetLength{" + "offset=" + offset + ", length=" + length + '}';
    }
    
}
