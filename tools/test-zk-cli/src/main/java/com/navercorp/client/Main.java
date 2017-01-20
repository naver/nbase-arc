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

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.navercorp.client.Client.Result;

public class Main {
    public static void main(String[] args) {
        Client clnt = null;
        try {
            CommandLine cmd = new DefaultParser().parse(new Options()
                    .addOption("z", true, "zookeeper address (ip:port,ip:port,...)")
                    .addOption("t", true, "zookeeper connection timeout").addOption("c", true, "command and arguments"),
                    args);

            final String connectionString = cmd.hasOption("z") ? cmd.getOptionValue("z") : "127.0.0.1:2181";
            final int timeout = cmd.hasOption("t") ? Integer.valueOf(cmd.getOptionValue("t")) : 10000;
            final String[] command = cmd.getArgs();

            clnt = new Client(connectionString, timeout);
            Result r = clnt.execute(command);

            if (r.message != null && r.message.length() > 0) {
                System.out.println(r.message);
            }
            if (r.error != null && r.error.length() > 0) {
                System.err.println(r.error);
            }
            System.exit(r.exitcode.n());
        } catch (ParseException e) {
            System.err.println(e.getStackTrace());
            System.err.println(Client.getUsage());
            System.exit(INVALID_ARGUMENT.n());
        } catch (IOException e) {
            System.err.println(e.getStackTrace());
            System.exit(ZK_CONNECTION_LOSS.n());
        } catch (InterruptedException e) {
            System.err.println(e.getStackTrace());
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
