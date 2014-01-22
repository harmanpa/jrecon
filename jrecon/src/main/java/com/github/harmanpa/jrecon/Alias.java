
package com.github.harmanpa.jrecon;

/**
 *
 * @author pete
 */
public class Alias {
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
        if ((this.transform == null) ? (other.transform != null) : !this.transform.equals(other.transform)) {
            return false;
        }
        return true;
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
