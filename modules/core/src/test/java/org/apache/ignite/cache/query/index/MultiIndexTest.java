package org.apache.ignite.cache.query.index;

import java.util.Objects;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.index.multi.MultiSortedIndex;
import org.apache.ignite.cache.query.index.multi.MultiSortedIndexFactory;
import org.apache.ignite.cache.query.index.sorted.Condition;
import org.apache.ignite.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.cache.query.index.sorted.SortedIndexFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class MultiIndexTest extends GridCommonAbstractTest {
    /** */
    protected IgniteIndexing idx;

    /** */
    @Before
    public void setUp() throws Exception {
        idx = new IgniteIndexing();
    }

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testDefaultIndexKey() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, String, Integer> func = new SortedIndexFunction<>(
            (k ,v) -> k, Integer.class
        );
        MultiSortedIndex<String> idx = createIndex(ignite, func);

        // Check empty index
        assertNull(idx.get(new Object[]{1}));

        // Check put item
        cache.put(1, "One");
        assertEquals("One", idx.get(new Object[]{1}));

        // Check update item
        cache.put(1, "NewOne");
        assertEquals("NewOne", idx.get(new Object[]{1}));

        // Check remove item
        cache.remove(1);
        assertNull(idx.get(new Object[]{1}));
    }

    /** TODO: for search under the multiple columns. */
    @Test
    public void testComplexObject() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, Person> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, Person, String> firstKeyFunc = new SortedIndexFunction<>(
            (k, v) -> v.firstName, String.class
        );
        SortedIndexFunction<Integer, Person, String> secondKeyFunc = new SortedIndexFunction<>(
            (k, v) -> v.lastName, String.class
        );
        SortedIndexFunction<Integer, Person, String> thirdKeyFunc = new SortedIndexFunction<>(
            (k, v) -> v.patronymic, String.class
        );

        MultiSortedIndex<Person> idx = createIndex(ignite, firstKeyFunc, secondKeyFunc, thirdKeyFunc);

        cache.put(1, new Person("Maksim", "Timonin", "Andreevich"));
        cache.put(4, new Person("Maksim", "Timonin", "Evgenievich"));
        cache.put(5, new Person("Maksim", "Efremov", "Petrovich"));
        cache.put(6, new Person("Petr", "Timonin", "Andreevich"));

        // Check exact match by 2 field
        Condition[] conditions = new Condition[2];
        conditions[0] = new Condition("Maksim", "Maksim");
        conditions[1] = new Condition("Timonin", "Timonin");

        GridCursor<Person> cursor = idx.find(conditions);

        assertTrue(cursor.next());
        assertEquals(new Person("Maksim", "Timonin", "Evgenievich"), cursor.get());

        assertTrue(cursor.next());
        assertEquals(new Person("Maksim", "Timonin", "Andreevich"), cursor.get());

        assertFalse(cursor.next());

        // Check exact match by 1 field
        conditions = new Condition[1];
        conditions[0] = new Condition("Maksim", "Maksim");

        cursor = idx.find(conditions);

        assertTrue(cursor.next());
        assertEquals(new Person("Maksim", "Timonin", "Evgenievich"), cursor.get());

        assertTrue(cursor.next());
        assertEquals(new Person("Maksim", "Timonin", "Andreevich"), cursor.get());

        assertTrue(cursor.next());
        assertEquals(new Person("Maksim", "Efremov", "Petrovich"), cursor.get());

        // Check range match by 1 field
        conditions = new Condition[1];
        conditions[0] = new Condition("Petr", "Maksim");

        cursor = idx.find(conditions);

        assertTrue(cursor.next());
        assertEquals(new Person("Petr", "Timonin", "Andreevich"), cursor.get());

        assertTrue(cursor.next());
        assertEquals(new Person("Maksim", "Timonin", "Evgenievich"), cursor.get());

        assertTrue(cursor.next());
        assertEquals(new Person("Maksim", "Timonin", "Andreevich"), cursor.get());

        assertTrue(cursor.next());
        assertEquals(new Person("Maksim", "Efremov", "Petrovich"), cursor.get());

        assertFalse(cursor.next());
    }

    private static class Person {
        private final String firstName;
        private final String lastName;
        private final String patronymic;

        private Person(String firstName, String lastName, String patronymic) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.patronymic = patronymic;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Person person = (Person)o;
            return Objects.equals(firstName, person.firstName) &&
                Objects.equals(lastName, person.lastName) &&
                Objects.equals(patronymic, person.patronymic);
        }

        @Override public int hashCode() {
            return Objects.hash(firstName, lastName);
        }

        @Override public String toString() {
            return "Person{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", patronymic='" + patronymic + '\'' +
                '}';
        }
    }

    /** */
    protected <CK, CV, IK extends Comparable<IK>> MultiSortedIndex<CV> createIndex(IgniteEx node,
        SortedIndexFunction<CK, CV, IK>... keyFuncs) {
        GridCacheContext<CK, CV> cctx = (GridCacheContext<CK, CV>) node.cachex("CACHE").context();

        SortedIndexDefinition def =
            new SortedIndexDefinition(cctx, "idx", 1, keyFuncs);

        return (MultiSortedIndex<CV>) idx
            .createIndex(new MultiSortedIndexFactory(), def)
            .unwrap(MultiSortedIndex.class);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setIndexingSpi(idx);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName("CACHE")
            .setQueryParallelism(1));

        return cfg;
    }
}
