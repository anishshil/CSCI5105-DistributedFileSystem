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

import java.io.*;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Server class which opens up a simple thrift server on a given host. Accepts
 * requests from the client for image processing tasks and delegates them to
 * compute nodes.
 */
public class Coordinator {

  public static NodeRequestHandler handler;

  public static CoordinatorService.Processor processor;
  
  public static List<FileServerNode> nodeList = new LinkedList<FileServerNode>();
  public static Map<String, List<FileServerNode>> ecMap1 = new HashMap<String, List<FileServerNode>>();
  public static Map<String, Integer> ecMap2 = new HashMap<String, Integer>();
  public static Map<String, String> ecMap3 = new HashMap<String, String>();

  // Hardcoded
  public static String coordIpAddr = "csel-w103-30.cselabs.umn.edu";
  public static int coordPort = 9091, coordNodeId = 0;

  public static int nr, nw, nTotal;
  public static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static void replicate() {

    Iterator<Map.Entry<String, List<FileServerNode>>> iter = ecMap1.entrySet().iterator();

    // Loop for each file which has been written but not replicated
    while (iter.hasNext()) {
      Map.Entry<String, List<FileServerNode>> pair = iter.next();
      // Acquire a writeLock
      lock.writeLock().lock();
      System.out.println("--------------------------------------------");
      System.out.println("Replicating changes for file " + pair.getKey());
      System.out.println("--------------------------------------------");

      List<FileServerNode> curList = pair.getValue();

      for (FileServerNode node : curList) {
        Request req = new Request();
        try {
          TTransport transport;
          
          transport = new TSocket(node.ipAddress, node.port);
          TProtocol protocol = new TBinaryProtocol(transport);
          CoordinatorService.Client client = new CoordinatorService.Client(protocol);
          transport.open();

          // Reading the latest file from the node
          System.out.println(
              "Replicating on " + node.nodeId + " for filename: " + pair.getKey() + "." + ecMap2.get(pair.getKey()));
          req.status = client.doWrite(pair.getKey() + "." + ecMap2.get(pair.getKey()), ecMap3.get(pair.getKey()));
          if (req.status == 1)
            System.out.println("Successfully replicated " + pair.getKey() + "." + ecMap2.get(pair.getKey())
                + " at node " + node.nodeId);
          else
            System.out.println("Failed to replicate on node: " + node.nodeId);

          transport.close();
        } catch (TException e) {
          e.printStackTrace();
        }

      }

      iter.remove();
      // Release the lock
      lock.writeLock().unlock();
    }

    // Sleep for about 15 secs before replicating again
    try {
      Thread.sleep(15000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Replicate recursively
    Coordinator.replicate();

  }

  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println(
          "Please specify the number of servers for read quorum (Nr), number of servers for write quorum (Nw), and total number of servers (Ntotal)");
      System.exit(1);
    }

    nr = Integer.parseInt(args[0]);
    nw = Integer.parseInt(args[1]);
    nTotal = Integer.parseInt(args[2]);

    try {
      handler = new NodeRequestHandler();
      processor = new CoordinatorService.Processor(handler);

      Runnable simple = new Runnable() {
        public void run() {
          simple(processor);
        }
      };

      new Thread(simple).start();
    } catch (Exception x) {
      x.printStackTrace();
    }

    File file = new File("/export/scratch/shil0037");
    if (!file.exists()) {
      if (file.mkdir()) {
        System.out.println("File Server directory named shil0037 is created in /export/scratch");
      } else {
        System.out.println("Failed to create directory!");
      }
    }

    // Start a new thread to replicate the contents of the file servers
    try {
      Runnable replicate = new Runnable() {
        public void run() {
          replicate();
        }
      };

      new Thread(replicate).start();
    } catch (Exception x) {
      x.printStackTrace();
    }

  }

  public static void simple(CoordinatorService.Processor processor) {
    FileServerNode coordinatorNode = new FileServerNode();

    coordinatorNode.nodeId = coordNodeId;
    coordinatorNode.port = coordPort;
    coordinatorNode.ipAddress = coordIpAddr;
    nodeList.add(coordinatorNode);

    try {
      TServerTransport serverTransport = new TServerSocket(coordPort);
      TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
      System.out.println("Starting the coordinator server...");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}