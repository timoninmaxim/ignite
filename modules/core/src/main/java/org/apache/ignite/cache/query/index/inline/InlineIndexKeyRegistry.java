package org.apache.ignite.cache.query.index.inline;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cache.query.index.inline.keys.IntegerInlineIndexKey;

public class InlineIndexKeyRegistry {

    private final Map<Class<?>, InlineIndexKey> mapping = new HashMap<>();

    public InlineIndexKeyRegistry() {
        mapping.put(Integer.class, new IntegerInlineIndexKey());
    }

    public InlineIndexKey get(Class<?> clazz) {
        return mapping.get(clazz);
    }
}
