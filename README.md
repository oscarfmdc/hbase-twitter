# hbase-twitter
Java application that stores trending topics from Twitter into HBase and provides users with a set of queries for data analysis

Start HBase - bin/start-hbase.sh
Start the shell - bin/hbase shell

Check at http://localhost:60010 that HBase is running

Standalone Configuration:

conf/hbase-site.xml:
<property>
	<name>hbase.rootdir</name>
	<value>file://OUR_ABSOLUTE_PATH/hbase-data/</value>
</property>
<property>	
	<name>hbase.zookeeper.property.dataDir</name>
	<value>PATH_FOR_ZKDATA</value>	
</property>

conf/hbase-env.sh
export JAVA_HOME=PATH_TO_JAVA_HOME
export HBASE_REGIONSERVER_OPTS="-XmsMINm -XmxMAXm"