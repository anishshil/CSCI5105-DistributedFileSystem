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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

// Generated code
import dfs.*;
import dfs.CoordinatorService.AsyncProcessor.assembleQuorum;

import java.io.*;
import java.math.*;
import java.net.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;

public class NodeRequestHandler implements CoordinatorService.Iface {

  private static int readOp = 0;
  private static int writeOp = 1;

  @Override
  public void ping() throws TException {
    System.out.println("Pinged the coordinator node...!");
  }

  // Always served by coordinator
  @Override
  public FileServerNode getRandomNode() {
    Random r = new Random();
    int low = 0;
    int high = Coordinator.nodeList.size() - 1;
    int idx = r.nextInt(high - low + 1) + low;

    return Coordinator.nodeList.get(idx);
  }

  @Override
  public Request requestWrite(String fileName, String content, int nodeId) {
    System.out.println("Called method requestWrite on node " + nodeId);
    Request req = new Request();

    if (nodeId == Coordinator.coordNodeId) {
      req = coordinate(fileName, writeOp, content);
    } else {
      try {
        TTransport transport;

        transport = new TSocket(Coordinator.coordIpAddr, Coordinator.coordPort);
        TProtocol protocol = new TBinaryProtocol(transport);
        CoordinatorService.Client client = new CoordinatorService.Client(protocol);
        transport.open();

        req = client.coordinate(fileName, writeOp, content);

        transport.close();
      } catch (TException x) {
        x.printStackTrace();
      }
    }

    return req;
  }

  @Override
  public Request requestRead(String fileName, int nodeId) {
    System.out.println("Called method requestRead on node " + nodeId);
    Request req = new Request();
    // Read operation; no content required
    String content = "";

    if (nodeId == Coordinator.coordNodeId) {
      req = coordinate(fileName, readOp, content);
    } else {
      try {
        TTransport transport;

        transport = new TSocket(Coordinator.coordIpAddr, Coordinator.coordPort);
        TProtocol protocol = new TBinaryProtocol(transport);
        CoordinatorService.Client client = new CoordinatorService.Client(protocol);
        transport.open();

        req = client.coordinate(fileName, readOp, content);

        transport.close();
      } catch (TException x) {
        x.printStackTrace();
      }
    }

    return req;
  }

  // The core function of the coordinator node. It will acquire read or write
  // locks based on the
  // type of operation requested by the client, and assemble a quorum based on the
  // limit set by
  // the coordinator.
  @Override
  public Request coordinate(String fileName, int op, String content) {
    System.out.println("Called coordinate function with fileName=" + fileName + ", op=" + op + ", content=" + content);
    Request req = new Request();

    if (op == readOp) {
      System.out.println("Requesting a read lock");
      Coordinator.lock.readLock().lock();
      System.out.println("Acquired a read lock");

      List<FileServerNode> readList = assembleQuorum(readOp);
      req = readLatestVersion(fileName, readList);

      Coordinator.lock.readLock().unlock();
      System.out.println("Read lock released");
      System.out.println("---------------------------------------------------");
      System.out.println("Read operation completed!");
      System.out.println("---------------------------------------------------");
    }

    else {
      System.out.println("Waiting to get a write lock");
      Coordinator.lock.writeLock().lock();
      System.out.println("Acquired a write lock");

      List<FileServerNode> writeList = assembleQuorum(writeOp);
      req = getHighestVersion(fileName, writeList, content);

      Coordinator.lock.writeLock().unlock();
      System.out.println("Released the write lock");
      System.out.println("---------------------------------------------------");
      System.out.println("Write operation completed!");
      System.out.println("---------------------------------------------------");
    }

    return req;
  }

  @Override
  public List<FileServerNode> assembleQuorum(int op) {
    List<FileServerNode> tempNodeList = new LinkedList<FileServerNode>();
    FileServerNode tempNode = new FileServerNode();
    System.out.println("Assembling " + (op == readOp ? "read" : "write") + " quorum now...");
    int quorumSize = (op == readOp ? Coordinator.nr : Coordinator.nw);

    while (tempNodeList.size() != quorumSize) {
      tempNode = getRandomNode();
      if (!tempNodeList.contains(tempNode))
        tempNodeList.add(tempNode);
    }

    System.out.print("Nodes in quorum for this op: " + op + " are: ");
    for (FileServerNode node : tempNodeList) {
      System.out.print(node.nodeId + " ");
    }

    System.out.println();
    return tempNodeList;
  }

  // This function takes the highest version of the file from the nodes in write
  // quorum
  // Increases that version by 1 and performs the write operation with new content
  @Override
  public Request getHighestVersion(String fileName, List<FileServerNode> writeList, String content) {
    System.out.println("Called function getHighestVersion");
    Request req = new Request();
    int maxVersion = -1, tempVersion = -1;

    for (FileServerNode node : writeList) {
      try {
        TTransport transport;

        transport = new TSocket(node.ipAddress, node.port);
        TProtocol protocol = new TBinaryProtocol(transport);
        CoordinatorService.Client client = new CoordinatorService.Client(protocol);
        transport.open();

        tempVersion = client.getFileVersion(fileName);

        transport.close();
      } catch (TException x) {
        x.printStackTrace();
      }

      if (tempVersion > maxVersion) {
        maxVersion = tempVersion;
      }
    }

    System.out.println("Max Version for " + fileName + " in Nw is " + maxVersion);
    maxVersion++;

    List<FileServerNode> tempList = new LinkedList<FileServerNode>(Coordinator.nodeList);
    tempList.removeAll(writeList);
    Coordinator.ecMap1.put(fileName, tempList);
    Coordinator.ecMap2.put(fileName, maxVersion);
    Coordinator.ecMap3.put(fileName, content);

    // perform write on each node in write list of quorum
    for (FileServerNode node : writeList) {
      try {
        TTransport transport;

        transport = new TSocket(node.ipAddress, node.port);
        TProtocol protocol = new TBinaryProtocol(transport);
        CoordinatorService.Client client = new CoordinatorService.Client(protocol);
        transport.open();

        // Writing on the nodes
        System.out.println("Calling doWrite on " + node.nodeId + " for filename: " + fileName + "." + maxVersion);
        req.status = client.doWrite(fileName + "." + maxVersion, content);
        req.content = "";

        transport.close();
      } catch (TException x) {
        x.printStackTrace();
      }
    }
    return req;
  }

  @Override
  public Request readLatestVersion(String fileName, List<FileServerNode> readList) {
    System.out.println("Called function readLatestVersion");

    Request req = new Request();
    int maxVersion = -1, tempVersion = -1;
    FileServerNode maxNode = new FileServerNode();

    for (FileServerNode node : readList) {
      // Calling all nodes in read quorum to get the max version of the file
      try {
        TTransport transport;

        transport = new TSocket(node.ipAddress, node.port);
        TProtocol protocol = new TBinaryProtocol(transport);
        CoordinatorService.Client client = new CoordinatorService.Client(protocol);
        transport.open();

        tempVersion = client.getFileVersion(fileName);

        transport.close();
      } catch (TException x) {
        x.printStackTrace();
      }

      if (tempVersion > maxVersion) {
        maxVersion = tempVersion;
        maxNode = node;
      }
    }

    System.out.println("Max version for file " + fileName + " is " + maxVersion);

    // Requested file is not present on any if the nodes in Read Quorum
    if (maxVersion == -1) {
      req.content = "";
      req.status = 0;
      return req;
    }

    System.out.println("Latest copy of requested file is present at node " + maxNode.nodeId);

    // Requested file is present on one of the nodes so reading from the latest
    // version
    try {
      TTransport transport;

      transport = new TSocket(maxNode.ipAddress, maxNode.port);
      TProtocol protocol = new TBinaryProtocol(transport);
      CoordinatorService.Client client = new CoordinatorService.Client(protocol);

      transport.open();

      // Reading the latest file from the node
      System.out.println("Calling doRead on node " + maxNode.nodeId + " for filename: " + fileName + "." + maxVersion);
      req.content = client.doRead(fileName + "." + maxVersion);
      req.status = 1;

      transport.close();
    } catch (TException x) {
      x.printStackTrace();
    }

    return req;

  }

  @Override
  public int getFileVersion(String fileName) {
    System.out.println("Called function getFileVersion for fileName " + fileName);

    File myDir = new File("/export/scratch/shil0037");
    File[] listOfFiles = myDir.listFiles();
    System.out.println("Number of files found: " + listOfFiles.length);
    String fName = "";

    for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].isFile()) {
        fName = listOfFiles[i].getName();

        if (fName.substring(0, fName.indexOf(".")).equals(fileName)) {
          return Integer.parseInt(fName.substring(fName.indexOf(".") + 1));
        }
      }
    }

    System.out.println("File " + fileName + " not found.");
    return -1;
  }

  @Override
  public String doRead(String fileName) {
    System.out.println("Called function doRead for file " + fileName);
    StringBuffer fileContent = new StringBuffer("");

    try {
      for (String line : Files.readAllLines(Paths.get("/export/scratch/shil0037", fileName),
          Charset.forName("US-ASCII")))
        fileContent.append(line);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return fileContent.toString();
  }

  @Override
  public int doWrite(String fileName, String content) {
    System.out.println("Called function doWrite for file " + fileName + " and content " + content);

    String pathToScan = "/export/scratch/shil0037";
    String targetFile;

    File dirToScan = new File(pathToScan);
    File[] listOfFiles = dirToScan.listFiles();
    int flag = 0;
    List<String> myList = new LinkedList<String>();

    for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].isFile()) {
        targetFile = listOfFiles[i].getName();

        if (targetFile.startsWith(fileName.substring(0, fileName.indexOf(".") + 1))) {
          File file = new File("/export/scratch/shil0037/" + targetFile);
          try {
            FileOutputStream fStream = new FileOutputStream(file, false);
            byte[] myBytes = content.getBytes();
            fStream.write(myBytes);
            file.renameTo(new File("/export/scratch/shil0037/" + fileName));
            fStream.close();
          } catch (Exception e) {
            e.printStackTrace();
          }

          System.out.println(fileName + " is written successfully to this node");
          flag = 1;
        }
      }
    }

    // No previous version of the file is found
    if (flag == 0) {
      try {
        File file = new File("/export/scratch/shil0037/" + fileName);
        file.createNewFile();

        FileWriter fwrite = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bufwrite = new BufferedWriter(fwrite);
        bufwrite.write(content);
        bufwrite.close();

        System.out.println(fileName + " is written successfully to this node");
        flag = 1;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    // Display the list of files after the operation
    myList = getFilesOnCurrentNode();
    System.out.println("-----------------------------------");
    System.out.println("List of files on current node:");
    for (String str : myList)
      System.out.print(str + " ");
    System.out.println();
    System.out.println("-----------------------------------");

    return flag;

  }

  @Override
  public int getNodeId(FileServerNode node) throws TException {
    System.out.println("Called function getNodeId");
    int tmpId = getNewId();
    // Repeatedly search nodeId to avoid ID collision
    while (searchId(tmpId)) {
      tmpId = getNewId();
    }

    // Add fileserver node to list of coordinator nodes
    node.nodeId = tmpId;
    Coordinator.nodeList.add(node);

    return node.nodeId;
  }

  // Searching the generated ID in the nodeList to avoid duplication of ID
  public boolean searchId(int tmpId) {

    if (Coordinator.nodeList == null)
      return false;

    for (FileServerNode node : Coordinator.nodeList) {
      if (node.nodeId == tmpId)
        return true;
    }

    return false;
  }

  public int getNewId() {
    Random r = new Random();
    int low = 1;
    int high = Coordinator.nTotal;

    return r.nextInt((high - low) + 1) + low;

  }

  @Override
  public List<String> getFilesOnCurrentNode() {
    System.out.println("Called function getFilesOnCurrentNode");

    File myDir = new File("/export/scratch/shil0037/");
    File[] listOfFiles = myDir.listFiles();
    List<String> myList = new LinkedList<String>();

    for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].isFile() && !listOfFiles[i].getName().equalsIgnoreCase("README.txt")) {
        myList.add(listOfFiles[i].getName());
      }
    }

    return myList;
  }

  public Map<Integer, List<String>> getFilesOnSystem() {
    Map<Integer, List<String>> fileState = new HashMap<Integer, List<String>>();
    for (FileServerNode node : Coordinator.nodeList) {

      try {
        TTransport transport;
        // Taking the ip and port number of the nodes in the list of activeNodes
        transport = new TSocket(node.ipAddress, node.port);
        TProtocol protocol = new TBinaryProtocol(transport);
        CoordinatorService.Client client = new CoordinatorService.Client(protocol);
        transport.open();

        fileState.put(node.nodeId, client.getFilesOnCurrentNode());

        transport.close();
      } catch (TException x) {
        x.printStackTrace();
      }

    }

    return fileState;
  }
}