# OpenTSDB in benchmark

This document is about how to install, deploy OpenTSDB, and how to test its inserting or querying performance by iotdb-benchmark. 

# Prerequisites

OpenTSDB should be installed in a linux system, the following prerequisites should be installed:

- JDK >= 1.6
- Gnuplot >= 4.2 
- ZooKeeper
- HBase >= 0.92

### Installation of the prerequisites

1. JDK

	environment parameter $JAVA_HOME should be set.

2. Gnuplot
	
	use ```sudo apt-get install gnuplot``` to install Gnuplot.

3. ZooKeeper
	
	(1) Download and decompress the installation file of zooKeeper, rename the file ```conf/zoo_sample.cfg``` to ```zoo.cfg```, use ```cp conf/zoo_sample.cfg conf/zoo.cfg```

	(2) Modify the ```dataDir``` patameter in the file ```conf/zoo.cfg``` to an accessible path.

	(3) Use ```bin/zkServer.sh start``` command to start zooKeeper. If the process is running successfully, you can see ```QuorumPeerMain``` process in jps.

	(4) Use ```bin/zkServer.sh stop``` to stop zooKeeper under the root directory.

4. HBase

	(1) Download and decompress the installation file of HBase, move into the file ```conf/hbase-env.sh```, modify ```HBASE_MANAGES_ZK``` property into false. And then, move into the file ```conf/hbase-site.xml``` , add the following code in ```<configuration></configuration>```:

	```
	<property>
        <name>hbase.cluster.distributed</name>
        <value>true</value>
    </property>
	```

	(2) Ensure the zooKeeper process is running, run ```bin/start-hbase.sh``` to run the HBase. 
	You can see the ```HMaster``` and ```HRegionServer``` process in jps while Hbase is running. You can also enter the bin folder and use ```./hbase shell``` to query the data in HBase.

	(3) Use ```bin/start-hbase.sh``` to stop HBase.


# Installation of OpenTSDB

1. Download and decompress the installation file of OpenTSDB，create build directory and move the third_party into it, use the following command:

```
mkdir build
cp -r third_party ./build
```

2. Build OpenTSDB: ```./build.sh```


3. Create the necessary tables in HBase by script:```Env COMPRESSION=NONE HBASE_HOME=/xxx/hbase-x.x.x ./src/create_table.sh```

4. Make the configuration of OpenTSDB:

	(1) create a configuration in build directory use the template, the command is ```mv src/opentsdb.conf build/opentsdb.conf```

	(2) Modify some parameters'value in ```build/opentsdb.conf```:
		Modify ```tsd.network.port``` 's value to 4242;
		Modify ```tsd.http.staticroot``` 's value to ```./staticroot```
		Modify ```tsd.http.cachedir``` 's value to an accessible path to store the cache.

5. Ensure the tables are created, the zooKeeper and Hbase is running, we can start OpenTSDB now. Move into the build directory, use ```./tsdb tsd``` to start the OpenTSDB, the jps process name of OpenTSDB is TSDMain.



# Use benchmark to test OpenTSDB

When using benchmark for testing, there are some differences between OpenTSDB and other databases.
You should modify the following parameters in ```conf/config.properties```:

```
DB_URL=http://your-server-path:4242
DB_SWITCH=OpenTSDB
```

And the ```OPERATION_PROPORTION``` parameter in ```conf/config.properties``` is like x1:x2:x3:x4:x5:x6:x7:x8:x9(where x1 to x9 are all numbers), when testing OpenTSDB, x4, x6, x7 should be zero because OpenTSDB can't query by values.

After finishing the modification, use the ```./benchmark.sh``` command to start the test.  
