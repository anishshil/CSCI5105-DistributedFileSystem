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
import java.net.*;
import java.util.*;

/**
 * Server class which opens up a simple thrift server on a given host. Accepts
 * requests from the client for image processing tasks and delegates them to
 * compute nodes.
 */
public class FileServer {

  public static FileServerNode myNode = new FileServerNode();

  public static String SuperNodeHost = Coordinator.coordIpAddr;
  public static int SuperNodePort = Coordinator.coordPort;

  // Handler to start the NodeServer
  public static NodeRequestHandler handler;
  public static CoordinatorService.Processor processor;

  public static void joinCoordinator() {
    try {
      TTransport transport;

      transport = new TSocket(SuperNodeHost, SuperNodePort);
      TProtocol protocol = new TBinaryProtocol(transport);
      CoordinatorService.Client client = new CoordinatorService.Client(protocol);
      transport.open();
      
      perform(client);

      transport.close();
    } catch (TException x) {
      x.printStackTrace();
    }
  }

  private static void perform(CoordinatorService.Client client) throws TException {
    System.out.println("Fetching ID for new node");
    myNode.nodeId = client.getNodeId(myNode);
    System.out.println("Preparing to start the server with ID " + myNode.nodeId);
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("Please specify the port number for the file server.");
      System.exit(1);
    }

    try {
      myNode.ipAddress = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }

    myNode.port = Integer.parseInt(args[0]);

    // Allow the file server to join the coordinator node
    joinCoordinator();

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

  }

  public static void simple(CoordinatorService.Processor processor) {
    try {
      TServerTransport serverTransport = new TServerSocket(myNode.port);
      TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
      System.out.println("Starting the file server...");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}