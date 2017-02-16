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
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;


public class Client {
    private final ZooKeeper zk;
    
    private final static String charset = "UTF-8";
    private final static Map<String, Command> commandMap;
    private final static String usage;
    private final static String cmdUsage;
    
    public enum Code {
        OK(0),

        ZK_CONNECTION_LOSS(10), 
        ZK_UNEXPECTED_ERROR(11), 
        ZK_NO_NODE(12), 
        ZK_NODE_EXISTS(13), 
        ZK_BAD_NODE_VERSION(14), 
        ZK_NO_CHILDREN_FOR_EPHEMERALS(15), 
        ZK_NOT_EMPTY_NODE(16),

        INVALID_COMMAND(50), 
        INVALID_ARGUMENT(51),

        INTERNAL_ERROR(100);

        private final int code;

        Code(final int c) {
            this.code = c;
        }

        public int n() {
            return code;
        }
    }

    public static class Result {
        public final String message;
        public final String error;
        public final Code exitcode;

        public Result(String so, String se, Code ec) {
            message = so;
            error = se;
            exitcode = ec;
        }
    }

    private interface Command {
        public Result execute(ZooKeeper zk, String[] args);
        public String usage();
        public int arity();
    }

    public Client(String connectionString, int timeoutms) throws IOException, InterruptedException {        
        final CountDownLatch sync = new CountDownLatch(1);
        zk = new ZooKeeper(connectionString, timeoutms, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.None && event.getState() == Event.KeeperState.SyncConnected) {
                    sync.countDown();
                }
            }
        });

        if (sync.await(timeoutms, TimeUnit.MILLISECONDS) == false) {
            zk.close();
            throw new IOException("Connection to zookeeper timed out. connectionString: " + connectionString);
        }
    }
    
    public void close() throws InterruptedException {
        zk.close();
    }

    public Result execute(String []command) {
        if (command.length == 0) {
            return new Result("", "Command length is 0", INVALID_COMMAND);
        }

        Command cmd = commandMap.get(command[0]);
        if (cmd == null) {
            return new Result("", "Command not found", INVALID_COMMAND);
        }
        
        if (cmd.arity() != command.length) {
            return new Result("", "Wrong number of arguments", INVALID_ARGUMENT);
        }

        try {
            return cmd.execute(zk, command);
        } catch (IllegalArgumentException e) {
            return new Result("", e.getMessage(), INVALID_ARGUMENT);
        }
    }
    
    public static String getUsage() {
        return usage;
    }
    
    private static List<String> listSubTreeBFS(ZooKeeper zk, final String path) throws KeeperException, InterruptedException {
        if (path.equals("/zookeeper")) {
            return new ArrayList<String>();
        }

        Deque<String> queue = new LinkedList<String>();
        List<String> tree = new ArrayList<String>();
        queue.add(path);
        tree.add(path);
        while (!queue.isEmpty()) {
            final String node = queue.pollFirst();
            for (final String child : zk.getChildren(node, false)) {
                final String childPath = (node.equals("/") ? node : node + "/") + child;
                if (childPath.equals("/zookeeper")) {
                    continue;
                }
                queue.add(childPath);
                tree.add(childPath);
            }
        }

        if (!tree.isEmpty() && tree.get(0).equals("/")) {
            tree.remove(0);
        }

        return tree;
    }

    static {
        Map<String, Command> map = new HashMap<String, Command>();
        map.put("create", new Command() {
            @Override
            public Result execute(ZooKeeper zk, String[] command) {
                if (command.length != 3) {
                    return new Result("", getUsage(), INVALID_ARGUMENT);
                }

                try {
                    zk.create(command[1], command[2].getBytes(Charset.forName(charset)), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                } catch (KeeperException e) {
                    return keeperExceptionResult(e);
                } catch (InterruptedException e) {
                    return new Result("", e.getMessage(), INTERNAL_ERROR);
                }

                return new Result("", "", OK);
            }
            
            @Override
            public String usage() {
                return "create <path> <data>";
            }
            
            @Override
            public int arity() {
                return 3;
            }
        });

        map.put("set", new Command() {
            @Override
            public Result execute(ZooKeeper zk, String[] command) {
                try {
                    zk.setData(command[1], command[2].getBytes(Charset.forName(charset)), -1);
                } catch (KeeperException e) {
                    return keeperExceptionResult(e);
                } catch (InterruptedException e) {
                    return new Result("", e.getMessage(), INTERNAL_ERROR);
                }

                return new Result("", "", OK);
            }

            @Override
            public String usage() {
                return "set <path> <data>";
            }
            @Override
            public int arity() {
                return 3;
            }
        });

        map.put("get", new Command() {
            @Override
            public Result execute(ZooKeeper zk, String[] command) {
                try {
                    return new Result(new String(zk.getData(command[1], false, null), charset), "", OK);
                } catch (UnsupportedEncodingException e) {
                    return new Result("", e.getMessage(), INVALID_ARGUMENT);
                } catch (KeeperException e) {
                    return keeperExceptionResult(e);
                } catch (InterruptedException e) {
                    return new Result("", e.getMessage(), INTERNAL_ERROR);
                }
            }
            
            @Override
            public String usage() {
                return "get <path>";
            }
            @Override
            public int arity() {
                return 2;
            }
        });

        map.put("del", new Command() {
            @Override
            public Result execute(ZooKeeper zk, String[] command) {
                try {
                    zk.delete(command[1], -1);
                    return new Result("", "", OK);
                } catch (KeeperException e) {
                    return keeperExceptionResult(e);
                } catch (InterruptedException e) {
                    return new Result("", "", INTERNAL_ERROR);
                }
            }
            
            @Override
            public String usage() {
                return "del <path>";
            }

            @Override
            public int arity() {
                return 2;
            }
        });

        map.put("rmr", new Command() {
            @Override
            public Result execute(ZooKeeper zk, String[] command) {
                try {
                    final List<String> childrenPath = listSubTreeBFS(zk, command[1]);
                    Collections.reverse(childrenPath);
                    for (String childPath : childrenPath) {
                        zk.delete(childPath, -1);
                    }
                    return new Result("", "", OK);
                } catch (KeeperException e) {
                    return keeperExceptionResult(e);
                } catch (InterruptedException e) {
                    return new Result("", "", INTERNAL_ERROR);
                }
            }
            
            @Override
            public String usage() {
                return "rmr <path>";
            }
            
            @Override
            public int arity() {
                return 2;
            }
        });

        map.put("ls", new Command() {
            @Override
            public Result execute(ZooKeeper zk, String[] command) {
                try {
                    final String children = zk.getChildren(command[1], false).toString();
                    return new Result(children.substring(1, children.length() - 1), "", OK);
                } catch (KeeperException e) {
                    return keeperExceptionResult(e);
                } catch (InterruptedException e) {
                    return new Result("", "", INTERNAL_ERROR);
                }
            }
            
            @Override
            public String usage() {
                return "ls <path>";
            }
            
            @Override
            public int arity() {
                return 2;
            }
        });
        
        map.put("help", new Command() {
            @Override
            public Result execute(ZooKeeper zk, String[] args) {
                return new Result(cmdUsage, "", OK);
            }

            @Override
            public String usage() {
                return "help";
            }

            @Override
            public int arity() {
                return 1;
            }
            
        });

        commandMap = Collections.unmodifiableMap(map);

        // Usage
        StringBuilder sb = new StringBuilder();
        
        for (Entry<String, Command> c : commandMap.entrySet()) {
            sb.append(c.getValue().usage()).append(" | ");
        };
        cmdUsage = sb.substring(0, sb.length() - 3);
        
        usage = "[USAGE]\njava -jar test-zk-cli.jar -z <host:port> -t <timeout>\n";
    }

    private static Result keeperExceptionResult(KeeperException e) {
        switch (e.code()) {
        case NONODE:
            return new Result("", e.getMessage(), ZK_NO_NODE);
        case NODEEXISTS:
            return new Result("", e.getMessage(), ZK_NODE_EXISTS);
        case NOTEMPTY:
            return new Result("", e.getMessage(), ZK_NOT_EMPTY_NODE);
        case NOCHILDRENFOREPHEMERALS:
            return new Result("", e.getMessage(), ZK_NO_CHILDREN_FOR_EPHEMERALS);
        case BADVERSION:
            return new Result("", e.getMessage(), ZK_BAD_NODE_VERSION);
        default:
            return new Result("", e.getMessage(), ZK_UNEXPECTED_ERROR);
        }
    }
}
