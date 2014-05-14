//package com.cstore.transToHive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class exeHiveQL {
	public static void main(String[] args) throws SQLException {

		if (args.length < 2) {
			System.out.print("��������Ҫ��ѯ����������־����  ����");
			System.exit(1);
		}

		String type = args[0];
		String date = args[1];

		// ��hive�д�����
		HiveUtil.createTable("create table if not exists loginfo11 ( rdate String,time ARRAY<string>,type STRING,relateclass STRING,information1 STRING,information2 STRING,information3 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':'");
		// ����hadoop��־�ļ���*��ʾ�������е���־�ļ�
		HiveUtil.loadDate("load data local inpath '/root/hadoop-1.2.1/logs/*.log.*' overwrite into table loginfo11");
		// ��ѯ���õ���Ϣ�������������ں���־���������Ϣ
		// ResultSet res1 =
		// HiveUtil.queryHive("select rdate,time[0],type,relateclass,information1,information2,information3 from loginfo11 where type='ERROR' and rdate='2011-07-29' ");
		//
		String str = "select rdate,time[0],type,relateclass,information1,information2,information3 from loginfo11 where type='"
				+ type + "' and rdate='" + date + "' ";
		System.out.println(str + "----test");
		ResultSet res1 = HiveUtil
				.queryHive("select rdate,time[0],type,relateclass,information1,information2,information3 from loginfo11 where type='"
						+ type + "' and rdate='" + date + "' ");
		// �������Ϣ����任�󱣴浽mysql�С�
		HiveUtil.hiveTomysql(res1);
		// ���رմ˴λỰ��hive����
		getConnect.closeHive();
		// �ر�mysql����
		getConnect.closemysql();

	}
}

