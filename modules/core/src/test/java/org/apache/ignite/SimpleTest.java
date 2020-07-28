package org.apache.ignite;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class SimpleTest extends GridCommonAbstractTest {

    @Test
    public void test() throws Exception {
        Ignite crd = startGrid();

        crd.createCache(
                new CacheConfiguration<>("testCache").setQueryEntities(
                        Collections.singletonList(
                                new QueryEntity()
                                        .setKeyFieldName("id")
                                        .setValueType("Person")
                                        .setFields(new LinkedHashMap<>(
                                                F.asMap("id", "java.lang.Integer",
                                                        "name", "java.util.UUID"))))));

        crd.binary().builder("Person")
                .removeField()

        crd.binary().builder("Person")
                .setField("id", 1)
                .setField("name", UUID.randomUUID())
                .build();

        crd.destroyCache("testCache");

        crd.createCache(
                new CacheConfiguration<>("testCache").setQueryEntities(
                        Collections.singletonList(
                                new QueryEntity()
                                        .setKeyFieldName("id")
                                        .setValueType("Person")
                                        .setFields(new LinkedHashMap<>(
                                                F.asMap("id", "java.lang.Integer",
                                                        "name", "java.lang.Long"))))));

        crd.binary().builder("Person")
                .setField("id", 1)
                .setField("name", 1234L)
                .build();
    }
}
