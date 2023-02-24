package org.apache.flink.streaming.examples.socket;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

/** nc -l equivalent. */
public class SocketWriter {

    public static void main(String[] args) {
        int port = Integer.valueOf(args[0]);
        boolean stop = false;
        ServerSocket serverSocket = null;
        Socket echoSocket = null;
        Scanner scanner = null;
        PrintWriter out = null;

        try {

            serverSocket = new ServerSocket(port);
            scanner = new Scanner(System.in);
            echoSocket = serverSocket.accept();
            out = new PrintWriter(echoSocket.getOutputStream(), true);
            while (!stop) {
                out.println(scanner.nextLine());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
                echoSocket.close();
                serverSocket.close();
                scanner.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
