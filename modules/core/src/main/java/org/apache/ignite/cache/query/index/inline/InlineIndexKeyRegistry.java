package org.apache.ignite.cache.query.index.inline;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.index.inline.keys.IntegerInlineIndexKey;
import org.apache.ignite.cache.query.index.inline.keys.StringInlineIndexKey;

public class InlineIndexKeyRegistry {

    private final Map<Class<?>, InlineIndexKey> mapping = new HashMap<>();

    public InlineIndexKeyRegistry() {
        mapping.put(Integer.class, new IntegerInlineIndexKey());
        // TODO: ignore case handling
        mapping.put(String.class, new StringInlineIndexKey(false));
    }

    public InlineIndexKey get(Class<?> clazz) {
        InlineIndexKey key = mapping.get(clazz);

        if (key == null)
            throw new IgniteException("There is no InlineIndexKey mapping for class " + clazz);

        return key;
    }
}
