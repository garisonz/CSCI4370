import java.util.HashSet;
import java.util.Set;

public class LinHashMultiMap<K, V> extends LinHashMap<K, Set<V>> {

    public LinHashMultiMap(Class<K> _classK, Class<Set<V>> _classV) {
        super(_classK, _classV);
    }

    @Override
    public Set<V> put(K key, Set<V> value) {
        var existingSet = get(key);
        if (existingSet == null) {
            existingSet = new HashSet<>();
            super.put(key, existingSet);
        }
        existingSet.addAll(value);
        return existingSet;
    }

    public void putSingle(K key, V value) {
        var existingSet = get(key);
        if (existingSet == null) {
            existingSet = new HashSet<>();
            super.put(key, existingSet);
        }
        existingSet.add(value);
    }
}
