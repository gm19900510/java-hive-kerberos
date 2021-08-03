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
		// ����jvm����ʱkrb5�Ķ�ȡ·������
		System.setProperty("java.security.krb5.conf", "krb5.conf");
		// ����kerberos��֤
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

		// ��ȡ���ݿ���Ϣ
		List<String> databases = client.getAllDatabases();
		System.out.println("���п�:  ");
		for (String database : databases) {
			System.out.println(database);
		}
		System.out.println();
		client.close();
	}
}