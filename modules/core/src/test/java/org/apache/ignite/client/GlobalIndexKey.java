package org.apache.ignite.client;

import java.io.Serializable;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/** */
public class GlobalIndexKey implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    @AffinityKeyMapped
    @QuerySqlField(index = true)
    private String value;

    @QuerySqlField
    private Object payload;

    public GlobalIndexKey(String value, Person payload) {
        this.value = value;
        this.payload = payload;
    }
}
