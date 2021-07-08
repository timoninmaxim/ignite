package org.apache.ignite.internal.ducktest.tests.gatling;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Random;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/** */
public class CacheInitApplication extends IgniteAwareApplication {

    public static final String CACHE = "PERSON_CACHE";

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        markInitialized();

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>()
            .setName(CACHE)
            .setIndexedTypes(Long.class, Person.class);

        ignite.createCache(ccfg);

        try(IgniteDataStreamer<Long, Person> streamer = ignite.dataStreamer(CACHE)) {
            Random r = new Random();

            for (int i = 0; i < 1_000_000; i++) {
                int age = 18 + r.nextInt(100);

                streamer.addData((long) i, new Person("Person " + i, age));
            }
        }

        markFinished();
    }

    /** */
    public static class Person {
        /** */
        @QuerySqlField(index = true)
        private int age;

        /** */
        @QuerySqlField
        private String name;

        /** */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
