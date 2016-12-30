## Test Setup

#### Requirements
We only checked these test cases on specific environment. Result of test on different environment is not guaranteed.

* Physical Machine (Xeon L5640 @ 2.27 X 2 socket, 48 GB Memory)
* CentOS 6.3 & CentOS 6.6
* python 2.7
* tcl 8.5+ required
* java 1.7

#### Build nbase-arc with gcov option.
```
$ make gcov

$ cd api/arcci
$ make gcov32

$ cd -
$ cd tools/local_proxy
$ make gcov32
```

#### Install zookeeper ensemble.
* Install three copies of zookeeper in ~/bin/zk1, ~/bin/zk2, and ~/bin/zk3  
  For integration test, these zookeeper paths are fixed in test case for zookeeper failure.

* Set up zookeeper config.

```
$ mv ~/bin/zk1/conf/zoo_sample.cfg ~/bin/zk1/conf/zoo.cfg
$ vi ~/bin/zk1/conf/zoo.cfg
    > server.1=127.0.0.1:2888:3887  
    > server.2=127.0.0.1:3888:4887  
    > server.3=127.0.0.1:4888:5887  
    >    
    > clientPort=2181  
    > dataDir=<home directory path>/bin/zk1/data
$ mkdir ~/bin/zk1/data
$ echo 1 > ~/bin/zk1/data/myid
$ cd ~/bin/zk1/bin
$ ./zkServer.sh start
```
```
$ mv ~/bin/zk2/conf/zoo_sample.cfg ~/bin/zk2/conf/zoo.cfg
$ vi ~/bin/zk2/conf/zoo.cfg
    > server.1=127.0.0.1:2888:3887  
    > server.2=127.0.0.1:3888:4887  
    > server.3=127.0.0.1:4888:5887  
    >    
    > clientPort=2182  
    > dataDir=<home directory path>/bin/zk2/data
$ mkdir ~/bin/zk2/data
$ echo 2 > ~/bin/zk2/data/myid
$ cd ~/bin/zk2/bin
$ ./zkServer.sh start
```
```
$ mv ~/bin/zk3/conf/zoo_sample.cfg ~/bin/zk3/conf/zoo.cfg
$ vi ~/bin/zk3/conf/zoo.cfg
    > server.1=127.0.0.1:2888:3887  
    > server.2=127.0.0.1:3888:4887  
    > server.3=127.0.0.1:4888:5887  
    >    
    > clientPort=2183
    > dataDir=<home directory path>/bin/zk3/data
$ mkdir ~/bin/zk3/data
$ echo 3 > ~/bin/zk3/data/myid
$ cd ~/bin/zk3/bin
$ ./zkServer.sh start
```
* Test zookeeper
```
$ cd ~/bin/zk1/bin
$ ./zkCli.sh -server localhost:2181
```
* Add zookeeper path to bashrc
```
$ vi ~/.bashrc
    > export ZOOKEEPER_HOME=~/bin/zk1
    > export PATH=$ZOOKEEPER_HOME/bin:$PATH
$ source ~/.bashrc
```

#### Set up ssh connection to sudo account
* Test program connects to sudo account via ssh while executing network isolation testcases.
* Set the environment variable, `$NBASE_ARC_TEST_SUDOER` as the name of sudo account.
* Run ssh-keygen
* Put the contents of generated public key to sudo account's authroized keys.  
  `(Example "cat ~/.ssh/id_rsa.pub >> ~$NBASE_ARC_TEST_SUDOER/.ssh/authorized_keys")`
* Test ssh connection
  `$ ssh <sudo account>@localhost ls /`


## Test Run
#### Run all testcases
```
$ python testrunner.py -n all
$ python testrunner.py -n -b all
```

#### Run nbase-arc processes without executing testcases.
```
$ python testrunner.py -i
```
  
#### Run a specific testcase.
```
$ python testrunner.py <testcase file name>
(Example "python testrunner.py test_restart_recovery.py")
```
