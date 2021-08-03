package com.gm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Kerberos {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";// jdbc驱动路径
	private static String url = "jdbc:hive2://node01.bigdata.hadoop:10000/default;principal=hive/node01.bigdata.hadoop@HADOOP.COM";// hive库地址+库名

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

	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConn();
			stmt = conn.createStatement();
			String sql = "show databases";
			ResultSet res = stmt.executeQuery(sql);
			System.out.println("所有库:  ");
			while (res.next()) {
				System.out.println(res.getString(1));
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
					stmt = null;
				}
				if (conn != null) {
					conn.close();
					conn = null;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static Connection getConn() throws ClassNotFoundException, SQLException {
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url);
		return conn;
	}
}
