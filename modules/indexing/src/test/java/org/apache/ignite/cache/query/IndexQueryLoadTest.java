package org.apache.ignite.cache.query;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class IndexQueryLoadTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void test() throws Exception {
        IgniteEx ign = startGrid(0);

        prepareTable();

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> futSql = multithreadedAsync(() -> {
            int cnt = 0;
            long start = System.currentTimeMillis();

            while (!stop.get()) {
                SqlFieldsQuery qry = new SqlFieldsQuery("select * from table_a where " +
                    "profileId < 100 " +
//                    "order by profileId desc, ts desc, eventId desc " +
                    "limit 5");

                qry.setLocal(true);

                List<List<?>> res = grid(0).context().query().querySqlFields(qry, false).getAll();

                cnt++;
            }

            System.out.println("SQL: Start=" + start + ", stop=" + System.currentTimeMillis() + ", cnt=" + cnt);

        }, 1, "sql-select");

//        IgniteInternalFuture<?> futIdxQry = multithreadedAsync(() -> {
//            int cnt = 0;
//            long start = System.currentTimeMillis();
//
//            while (!stop.get()) {
//                IndexQuery qry = new IndexQuery("Person", "index_a");
//                qry.setPartition(0);
////                qry.setLocal(true);
//
//                grid(0).cache(DEFAULT_CACHE_NAME).withKeepBinary().query(qry).getAll();
//
//                cnt++;
//            }
//
//            System.out.println("INDEX: Start=" + start + ", stop=" + System.currentTimeMillis() + ", cnt=" + cnt);
//
//        }, 1, "idx-qry");

//        IgniteInternalFuture<?> scanQry = multithreadedAsync(() -> {
//            int cnt = 0;
//            long start = System.currentTimeMillis();
//
//            while (!stop.get()) {
//                ScanQuery qry = new ScanQuery<>();
//                qry.setLocal(true);
//
//                grid(0).cache(DEFAULT_CACHE_NAME).withKeepBinary().query(qry).getAll();
//
//                cnt++;
//            }
//
//            System.out.println("SCAN: Start=" + start + ", stop=" + System.currentTimeMillis() + ", cnt=" + cnt);
//
//        }, 1, "scan-qry");

        Thread.sleep(30_000);

        stop.set(true);

        futSql.get();
//        futIdxQry.get();
//        scanQry.get();
    }

    /** */
    private void prepareTable() {
        SqlFieldsQuery qry = new SqlFieldsQuery("create table table_a (" +
            "id int PRIMARY KEY, " +
            "profileId int, " +
            "ts long," +
            "eventId long)" +
            " with \"VALUE_TYPE=Person,CACHE_NAME=" + DEFAULT_CACHE_NAME + "\";");

        grid(0).context().query().querySqlFields(qry, false).getAll();

        qry = new SqlFieldsQuery("create index index_a on table_a (profileId desc, ts desc, eventId desc);");

        grid(0).context().query().querySqlFields(qry, false).getAll();

        qry = new SqlFieldsQuery("insert into table_a (id, profileId, ts, eventId) values(?, ?, ?, ?);");

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 10_000; i++) {
            qry.setArgs(i, rnd.nextInt(500), System.currentTimeMillis(), i);

            grid(0).context().query().querySqlFields(qry, false).getAll();
        }
    }
}
