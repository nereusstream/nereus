# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ARG NEREUS_ADMIN_BASE_IMAGE
FROM ${NEREUS_ADMIN_BASE_IMAGE}

ARG NEREUS_COMMIT
LABEL org.opencontainers.image.version="v0.1.0"
LABEL org.opencontainers.image.revision="${NEREUS_COMMIT}"

WORKDIR /opt/nereus-admin

COPY nereus-admin/build/install/nereus-admin/ /opt/nereus-admin/

USER 10000:0

ENTRYPOINT ["/opt/nereus-admin/bin/nereus-admin"]
