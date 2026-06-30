#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# Route to the correct service binary based on first arg or NODE_ROLE env var.
# All four roles share the same image.
if [[ "$1" == "nameserver" || "${NODE_ROLE}" == "nameserver" ]]; then
  shift
  exec ./mqnamesrv "${@}"
elif [[ "$1" == "broker" || "${NODE_ROLE}" == "broker" ]]; then
  shift
  exec ./mqbroker "${@}"
elif [[ "$1" == "proxy" || "${NODE_ROLE}" == "proxy" ]]; then
  shift
  exec ./mqproxy "${@}"
elif [[ "$1" == "controller" || "${NODE_ROLE}" == "controller" ]]; then
  shift
  exec ./mqcontroller "${@}"
else
  exec "$@"
fi
