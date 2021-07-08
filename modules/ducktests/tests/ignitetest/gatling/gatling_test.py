# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module contains basic ignite test.
"""

from ignitetest.gatling.gatling_service import GatlingService
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, V_2_10_0


class GatlingTest(IgniteTest):
    """
    Basic Gatling test.
    """

    @ignite_versions(str(V_2_10_0))
    def gatling_test(self, ignite_version):
        # Start Ignite cluster.
        configuration = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            self_monitoring=True,
            metric_exporter="org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi",
            client_connector_configuration=ClientConnectorConfiguration())

        ignite = IgniteService(self.test_context, configuration, startup_timeout_sec=180, num_nodes=1)

        ignite.start()

        # Create cache, fill cache with data.
        client_configuration = configuration._replace(client_mode=True,
                                                      discovery_spi=from_ignite_cluster(ignite))

        app = IgniteApplicationService(
            self.test_context,
            client_configuration,
            java_class_name="org.apache.ignite.internal.ducktest.tests.gatling.CacheInitApplication"
        )
        app.start()
        app.await_stopped()

        self.run_gatling(configuration)
        self.run_gatling(configuration)
        self.run_gatling(configuration)

        # Stop Ignite cluster.
        ignite.stop()

    def run_gatling(self, configuration):
        # Start performance test.
        gatling = GatlingService(
            self.test_context,
            configuration,
            simulation_class_name="org.apache.ignite.gatling.simulations.QueryComparisonSimulation")

        gatling.run()
        gatling.await_stopped()
