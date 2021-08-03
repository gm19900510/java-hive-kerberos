package com.gm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import java.io.IOException;
import java.util.List;

public class MetaStore {

	static {
		// 设置jvm启动时krb5的读取路径参数
		System.setProperty("java.security.krb5.conf", "krb5.conf");
		// 配置kerberos认证
		Configuration conf = new Configuration();
		conf.setBoolean("hadoop.security.authorization", true);
		conf.set("hadoop.security.authentication", "kerberos");
		UserGroupInformation.setConfiguration(conf);
		try {
			UserGroupInformation.loginUserFromKeytab("hive/node01.bigdata.hadoop@HADOOP.COM", "hive.keytab");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws TException {

		HiveConf hiveConf = new HiveConf();

		hiveConf.addResource("hive-site.xml");
		HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);

		// 获取数据库信息
		List<String> databases = client.getAllDatabases();
		System.out.println("所有库:  ");
		for (String database : databases) {
			System.out.println(database);
		}
		System.out.println();
		client.close();
	}
}