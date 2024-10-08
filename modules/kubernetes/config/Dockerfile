#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Use Java 8 image as default one.
FROM openjdk:11

# Set Apache Ignite configuration file name.
ARG IGNITE_CFG_XML="example-kube.xml"

# Set Apache Ignite version.
ARG IGNITE_VERSION="2.0.0"

# Set IGNITE_HOME variable.
ENV IGNITE_HOME /opt/ignite/apache-ignite-${IGNITE_VERSION}-bin

# Set a path to the Apache Ignite configuration file. Use the run.sh script below:
ENV CONFIG_URI ${IGNITE_HOME}/config/$IGNITE_CFG_XML

# Make sure the Kubernetes lib is copied to the 'libs' folder.
ENV OPTION_LIBS ignite-kubernetes

# Disabling quiet mode.
ENV IGNITE_QUIET=false

# Install or update needed tools.
RUN apt-get update && apt-get install -y --no-install-recommends unzip

# Creating and setting a working directory for following commands.
WORKDIR /opt/ignite

# Copying local Apache Ignite build to the docker image.
COPY ./apache-ignite-${IGNITE_VERSION}-bin.zip apache-ignite-${IGNITE_VERSION}-bin.zip

# Unpacking the build.
RUN unzip apache-ignite-${IGNITE_VERSION}-bin.zip
RUN rm apache-ignite-${IGNITE_VERSION}-bin.zip

# Copying the executable file and setting permissions.
COPY ./run.sh $IGNITE_HOME/
RUN chmod +x $IGNITE_HOME/run.sh

# Copy the configuration.
COPY ./$IGNITE_CFG_XML $IGNITE_HOME/config

# Start an Apache Ignite node.
CMD $IGNITE_HOME/run.sh

# Exposing the ports.
EXPOSE 11211 47100 47500 49112

