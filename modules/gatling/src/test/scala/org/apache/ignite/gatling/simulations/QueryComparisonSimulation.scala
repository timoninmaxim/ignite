/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.gatling.simulations

import io.gatling.core.Predef._
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.protocol.IgniteProtocol

import scala.util.Random;

/**
 * SqlFieldsQuery vs IndexQuery simulation.
 */
class QueryComparisonSimulation extends Simulation {
    val personCache = "PERSON_CACHE"

    val tagSuffix = new Random().nextString(3);

    val sqlScenario = buildSqlScenario(tagSuffix)
//    val idxScenario = buildIdxScenario(tagSuffix)

    setUp(
        sqlScenario.inject(constantConcurrentUsers(1) during 180)
//            .andThen(idxScenario.inject(constantConcurrentUsers(1) during 60))
    ).protocols(new IgniteProtocol(serverNode))

    after(serverNode.close())

//    private def buildIdxScenario(suffix: String) = {
//        val idxQry = exec(
//            ignite("IndexQuery-tag-" + suffix)
//                .cache(personCache)
//                .apply(igniteCache => {
//                    val q = new IndexQuery[Long, Person](classOf[Person])
//                        .setCriteria(IndexQueryCriteriaBuilder.lt("age", 20))
//
//                    igniteCache.query(q).getAll
//                }))
//
//        scenario("IndexQuery-scn").exec(idxQry)
//    }

    private def buildSqlScenario(suffix: String) = {
        val sqlQuery = exec(
            ignite("SqlFieldsQuery-tag-" + suffix)
                .cache(personCache)
                .apply(igniteCache => {
                    igniteCache
                        .query(new SqlFieldsQuery("select _KEY, _VAL from Person where age < 20"))
                        .getAll
                }))

        scenario("SqlFieldsQuery-scn").exec(sqlQuery)
    }
}


