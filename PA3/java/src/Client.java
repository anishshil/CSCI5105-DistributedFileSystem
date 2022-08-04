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
 * specific languagessssss governing permissions and limitations
 * under the License.
 */

// Generated code
import dfs.*;

import java.io.*;
import java.util.*;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

/**
 * Client class to perform lookup and insert operations on the DHT nodes. The
 * client will either insert key-value pairs of word-meanings to the DHT, or
 * lookup the meaning of a particular word.
 */
public class Client {

  public static List<String> fileNames = Arrays.asList("Apple", "Banana", "Grapes", "Orange", "Pineapple");
  public static int rc = 0, wc = 0;
  public static float readTime, writeTime;

  public static void requestOp(int op, String fileName, String content) {
    FileServerNode fsNode = new FileServerNode();
    TTransport transport;

    try {
      transport = new TSocket(Coordinator.coordIpAddr, Coordinator.coordPort);
      TProtocol protocol = new TBinaryProtocol(transport);
      CoordinatorService.Client client = new CoordinatorService.Client(protocol);
      transport.open();

      fsNode = client.getRandomNode();

      transport.close();
    } catch (TException x) {
      x.printStackTrace();
    }

    try {
      transport = new TSocket(fsNode.ipAddress, fsNode.port);
      TProtocol protocol = new TBinaryProtocol(transport);
      CoordinatorService.Client client = new CoordinatorService.Client(protocol);
      transport.open();

      if (op == 1) {
        // Increment the write operation counter
        final long startTime = System.currentTimeMillis();
        Request req = client.requestWrite(fileName, content, fsNode.nodeId);
        final long endTime = System.currentTimeMillis();
        wc++;

        writeTime += endTime - startTime;

        if (req.status == 1)
          System.out.println("\nWrite successful!\n" + req.content);
      } else if (op == 0) {
        // Increment the read operation counter
        final long startTime = System.currentTimeMillis();
        Request req = client.requestRead(fileName, fsNode.nodeId);
        final long endTime = System.currentTimeMillis();
        rc++;

        readTime += endTime - startTime;

        if (req.status == 1)
          System.out.println("\nRead successful! \n Content is: " + req.content);
        else if (req.status == 0)
          System.out.println("\nRead Unsuccessful\n");
      }

      transport.close();
    } catch (TException x) {
      x.printStackTrace();
    }

  }

  public static void main(String args[]) {
    if (args.length != 2) {
      System.out.println(
          "Please specify an operation code and the number of operations. Use the following legend to decide the operation type by the code:");
      System.out.println("1 -> read heavy client");
      System.out.println("2 -> write heavy client");
      System.out.println("3 -> equal reads and writes (balanced load)");
      System.out.println("4 -> get filesystem state");
      System.exit(1);
    }

    final int op = Integer.parseInt(args[0]);
    final int numberOfOps = Integer.parseInt(args[1]);
    int fileCount = fileNames.size();

    /**
     * op=0 -> read heavy client op=1 -> write heavy client op=2 -> equal number of
     * reads and writes (balanced load) op=3 -> print file system state
     */
    if (op == 0 || op == 1) {
      for (int i = 1; i <= numberOfOps; i++) {
        final int t = i;
        try {
          Runnable requestOp = new Runnable() {
            public void run() {
              if (op == 0) {
                final int r = (t % 10 == 0) ? 1 : 0;
                // Switching the value of r provides negative test case
                // final int r = 0;
                requestOp(r, fileNames.get(t % fileCount), fileNames.get(t % fileCount));
              } else {
                final int r = (t % 10 == 0) ? 0 : 1;
                // final int r = 1;
                requestOp(r, fileNames.get(t % fileCount), fileNames.get(t % fileCount));
              }
            }
          };

          new Thread(requestOp).start();
        } catch (Exception x) {
          x.printStackTrace();
        }
      }
    }

    if (op == 2) {
      for (int i = 1; i <= numberOfOps; i++) {
        final int t = i;
        try {
          Runnable requestOp = new Runnable() {
            public void run() {
              requestOp(t % 2, fileNames.get(t % fileCount), fileNames.get(t % fileCount));
            }
          };

          new Thread(requestOp).start();
        } catch (Exception x) {
          x.printStackTrace();
        }
      }

    }

    if (op == 3) {
      try {
        TTransport transport;

        transport = new TSocket(Coordinator.coordIpAddr, Coordinator.coordPort);
        TProtocol protocol = new TBinaryProtocol(transport);
        CoordinatorService.Client client = new CoordinatorService.Client(protocol);

        transport.open();

        Map<Integer, List<String>> fileSystemState = client.getFilesOnSystem();

        for (Map.Entry<Integer, List<String>> entry : fileSystemState.entrySet()) {
          Integer nodeId = entry.getKey();
          List<String> listOfFilesOnNode = entry.getValue();
          System.out.println("\nThe files on the node " + nodeId.intValue() + " are: ");
          for (String file : listOfFilesOnNode) {
            System.out.print(file + " ");
          }
          System.out.println();
        }

        transport.close();
      } catch (TException x) {
        x.printStackTrace();
      }
    }

    if (op == 0 || op == 1 || op == 2) {
      while (rc + wc < numberOfOps) {
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      System.out.println("Total reads: " + rc);
      System.out.println("Total writes: " + wc);
      System.out.println("Total time: " + (writeTime + readTime));
    }
  }
}