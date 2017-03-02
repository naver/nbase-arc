/*
 * Copyright 2015 Naver Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.client;

import static com.navercorp.client.Client.Code.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.codehaus.jackson.map.ObjectMapper;

import com.navercorp.client.Client.Code;
import com.navercorp.client.Client.Result;
 
public class Main {
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public static void printResult(Result r) {
        try {
            System.out.println(mapper.writeValueAsString(r));
        } catch (IOException e) {
            e.printStackTrace(System.err);
            System.err.println(e.getMessage());
            System.exit(INTERNAL_ERROR.n());
        }
    }
    
    public static void main(String[] args) {
        Client clnt = null;
        try {
            CommandLine cmd = new DefaultParser().parse(new Options()
                    .addOption("z", true, "zookeeper address (ip:port,ip:port,...)")
                    .addOption("t", true, "zookeeper connection timeout").addOption("c", true, "command and arguments"),
                    args);

            final String connectionString = cmd.hasOption("z") ? cmd.getOptionValue("z") : "127.0.0.1:2181";
            final int timeout = cmd.hasOption("t") ? Integer.valueOf(cmd.getOptionValue("t")) : 10000;
            clnt = new Client(connectionString, timeout);

            String command;
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            while ((command = in.readLine()) != null) {
                printResult(clnt.execute(command.split(" ")));
            }
            System.exit(Code.OK.n());
        } catch (ParseException e) {
            e.printStackTrace(System.err);
            System.err.println(Client.getUsage());
            System.exit(INVALID_ARGUMENT.n());
        } catch (IOException e) {
            e.printStackTrace(System.err);
            System.exit(ZK_CONNECTION_LOSS.n());
        } catch (InterruptedException e) {
            e.printStackTrace(System.err);
            System.exit(INTERNAL_ERROR.n());
        } finally {
            if (clnt != null) {
                try {
                    clnt.close();
                } catch (Exception e) {
                    ; // nothing to do
                }
            }
        }
    }
}
