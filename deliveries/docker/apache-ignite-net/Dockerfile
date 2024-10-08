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

# Build runtime image
FROM microsoft/dotnet:runtime

## Workaround for apt/jre issues
RUN mkdir -p /usr/share/man/man1
RUN apt update && apt install apt-utils -y --no-install-recommends

## Install JRE
RUN apt update && apt install openjdk-11-jre-headless -y --no-install-recommends

WORKDIR /app

COPY libs ./libs
COPY publish ./

# Container port exposure
EXPOSE 11211 47100 47500 49112 10800 8080

ENTRYPOINT ["dotnet", "Apache.Ignite.dll"]
