package com.hbase.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;


/**
 * 测试连接到服务器
 */
public class demo1 {


    public static void main(String[] args) throws IOException {

        //1.获取配置对象
        Configuration configuration=new Configuration();
        //1.1设置连接hbase连接信息
        configuration.set("hbase.zookeeper.quorum","192.168.125.22:2181");
        //2.获取核心对象admin
        HBaseAdmin admin= (HBaseAdmin) ConnectionFactory.createConnection(configuration).getAdmin();
        //3.通过admin对象测试
        boolean flag = admin.tableExists(TableName.valueOf("Students"));
        //4。打印
        System.out.println(flag);
        //5.释放资源
        admin.close();

    }

}
