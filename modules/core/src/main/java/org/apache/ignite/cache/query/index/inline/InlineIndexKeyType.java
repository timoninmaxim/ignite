package org.apache.ignite.cache.query.index.inline;


// TODO: enum vs public static int fields
// TODO: types got from org.h2.Value for backward compatibility
public enum InlineIndexKeyType {
    /** The data type is unknown at this time. */
    UNKNOWN(-1),

    /** The value type for NULL. */
    NULL(0),

    /** The value type for BOOLEAN values. */
    BOOLEAN(1),

    /** The value type for BYTE values. */
    BYTE(2),

    /** The value type for SHORT values. */
    SHORT(3),

    /** The value type for INT values. */
    INT(4),

    /** The value type for INT values. */
    LONG(5),

    /** The value type for DECIMAL values. */
    DECIMAL(6),

    /** The value type for DOUBLE values. */
    DOUBLE(7),

    /** The value type for FLOAT values. */
    FLOAT(8),

    /** The value type for TIME values. */
    TIME(9),

    /**
     * The value type for DATE values.
     */
    DATE(10),

    /**
     * The value type for TIMESTAMP values.
     */
    TIMESTAMP(11),

    /**
     * The value type for BYTES values.
     */
    BYTES(12),

    /**
     * The value type for STRING values.
     */
    STRING(13),

    /**
     * The value type for case insensitive STRING values.
     */
    STRING_IGNORECASE(14),

    /**
     * The value type for BLOB values.
     */
    BLOB(15),

    /**
     * The value type for CLOB values.
     */
    CLOB(16),

    /**
     * The value type for ARRAY values.
     */
    ARRAY(17),

    /**
     * The value type for RESULT_SET values.
     */
    RESULT_SET(18),

    /**
     * The value type for JAVA_OBJECT values.
     */
    JAVA_OBJECT(19),

    /**
     * The value type for UUID values.
     */
    UUID(20),

    /**
     * The value type for string values with a fixed size.
     */
    STRING_FIXED(21),

    /**
     * The value type for string values with a fixed size.
     */
    GEOMETRY(22),

    // 23 was a short-lived experiment "TIMESTAMP UTC" which has been removed.

    /**
     * The value type for TIMESTAMP WITH TIME ZONE values.
     */
    TIMESTAMP_TZ(24),

    /**
     * The value type for ENUM values.
     */
    ENUM(25);

    private final int type;

    InlineIndexKeyType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
