package com.hbase.demo;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

public class demo1 {
    public static void main(String[] args) throws IOException {
      /*  //1.获取配置对象
        Configuration configuration=new Configuration();
        //1.1设置连接hbase连接信息
        configuration.
        configuration.set("hbase.zookeeper.quorum","192.168.125.22:2181");
        //2.获取核心对象admin
        HBaseAdmin admin= (HBaseAdmin) ConnectionFactory.createConnection(configuration).getAdmin();
        //3.通过admin对象测试
        boolean flag = admin.tableExists(TableName.valueOf("Students"));
        //4。打印
        System.out.println(flag);
        //5.释放资源
        admin.close();*/

        //第一步，设置HBsae配置信息
        Configuration configuration = HBaseConfiguration.create();
        //注意。这里这行目前没有注释掉的，这行和问题3有关系  是要根据自己zookeeper.znode.parent的配置信息进行修改。
        configuration.set("zookeeper.znode.parent","/hbase223"); //与 hbase-site-xml里面的配置信息 zookeeper.znode.parent 一致
        configuration.set("hbase.zookeeper.quorum","192.168.125.22");  //hbase 服务地址
        configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        //这里使用的是接口Admin   该接口有一个实现类HBaseAdmin   也可以直接使用这个实现类
        // HBaseAdmin baseAdmin = new HBaseAdmin(configuration);
        Admin admin = ConnectionFactory.createConnection(configuration).getAdmin();
        if(admin !=null){
            try {
                Table table = HbaseUtils.getTable();
                Get get=new Get(Bytes.toBytes("001"));
                Result result=table.get(get);
                NavigableMap<byte[], byte[]> grades =
                        result.getFamilyMap(Bytes.toBytes("Grades"));
                for(Map.Entry<byte[],byte[]> entry:grades.entrySet()){
                    String k=new String(entry.getKey());
                    String v=new String(entry.getValue());
                    System.out.println(k+" : "+v);
                }

            }catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    }
