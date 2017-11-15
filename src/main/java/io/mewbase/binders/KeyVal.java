package io.mewbase.binders;


import java.util.Objects;

/**
 * Container for Keys and Values of any Kind
 */
public class KeyVal<K, V> {

    public final K key;
    public final V value;

    /**
     * Constructor for a Pair.
     *
     * @param key - The key
     * @param value - The associated value
     */
    public KeyVal(K key, V value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Checks the two objects for equality by delegating to their respective
     * {@link Object#equals(Object)} methods.
     *
     * @param o the {@link KeyVal} to which this one is to be checked for equality
     * @return true if the underlying objects of the Pair are both considered
     *         equal
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KeyVal)) {
            return false;
        }
        KeyVal<?, ?> p = (KeyVal<?, ?>) o;
        return Objects.equals(p.key, key) && Objects.equals(p.value, value);
    }

    /**
     * Compute a hash code using the hash codes of the underlying objects
     *
     * @return a hashcode of the KV Pair
     */
    @Override
    public int hashCode() {
        return (key == null ? 0 : key.hashCode()) ^ (value == null ? 0 : value.hashCode());
    }

    /**
     * Convenience method for creating an appropriately typed KV pair.
     * @param K the key
     * @param V the  value
     * @return a Pair that is templatized with the types of a and b
     */
    public static <K, V> KeyVal <K, V> create(K k, V v) {
        return new KeyVal<K, V>(k, v);
    }

}
