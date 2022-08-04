/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
include "fsnode.thrift"

namespace java dfs

typedef i32 int

struct Request {
    1: string content,
    2: int status
}

service CoordinatorService {

    void ping(),

    int getNodeId(1: fsnode.FileServerNode node),
    fsnode.FileServerNode getRandomNode(),

    Request coordinate(1: string fileName, 2: int op, 3: string content),
    list<fsnode.FileServerNode> assembleQuorum(1: int op),
    int getFileVersion(1: string fileName),
    list<string> getFilesOnCurrentNode(),
    map<int, list<string>> getFilesOnSystem(),

    // Read operations
    Request requestRead(1: string fileName, 2: int nodeId),
    Request readLatestVersion(1: string fileName, 2: list<fsnode.FileServerNode> nrList),
    string doRead(1: string fileName),

    // Write operations
    Request requestWrite(1: string fileName, 2: string content, 3: int nodeId),
    Request getHighestVersion(1: string fileName, 2: list<fsnode.FileServerNode> nwList, 3: string content),
    int doWrite(1: string fileName, 2: string content)

}
