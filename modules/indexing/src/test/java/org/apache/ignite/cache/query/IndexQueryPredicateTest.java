package org.apache.ignite.cache.query;

import java.util.List;
import java.util.Objects;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IndexQueryPredicateTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private IgniteCache cache;

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignite ignite = startGrids(4);

        try (IgniteDataStreamer<Long, Person> s = ignite.dataStreamer(CACHE)) {
            for (int i = 0; i < 10000; i++)
                s.addData((long) i, new Person(i));
        }

        cache = ignite.cache(CACHE);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Long, Person> ccfg = new CacheConfiguration<Long, Person>()
            .setName(CACHE)
            .setIndexedTypes(Long.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    @Test
    public void test() {
        IndexQuery<Long, Person> qry = IndexQuery
            .<Long, Person>forType(Person.class)
            .lt("id", 100)
            .predicate((k, v) -> v.id < 10);

        List<Cache.Entry<Long, Person>> result = cache.query(qry).getAll();

        assertEquals(10, result.size());
    }

    // TODO: public methods, what to do with private, final, lack of default constructor?
    // TODO:

    /** */
    public static class Person {
        /** */
        @QuerySqlField(index = true)
        /* TODO final*/ public int id;

        public Person() {}

        /** */
        Person(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person[id=" + id + "]";
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person) o;

            return Objects.equals(id, person.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id);
        }
    }
}
