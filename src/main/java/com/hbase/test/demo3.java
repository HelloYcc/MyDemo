package com.hbase.test;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Hbase DDL 操作
 */
public class demo3 {
    /**
     * create 表操作
     */

    public void create_table() throws IOException {
        //1.获取到admin对象
        Admin admin = HbaseUtils.getAdmin();
        //2.创建表的描述器
        HTableDescriptor hTableDescriptor=new HTableDescriptor(TableName.valueOf("student"));
        //3.指定列簇
        HColumnDescriptor columnDescriptor=new HColumnDescriptor("Grades");
        hTableDescriptor.addFamily(columnDescriptor);
        //4.建表
        admin.createTable(hTableDescriptor);
        //5.释放资源
        HbaseUtils.close(admin);

    }


    /**
     * alter  修改表操作
     */
    public void alter_table() throws IOException {
        //1.获取admin对象
        Admin admin=HbaseUtils.getAdmin();
        //2.获取表的描述器
        HTableDescriptor hTableDescriptor=new HTableDescriptor(TableName.valueOf("students"));
        //3.指定列簇
        HColumnDescriptor hColumnDescriptor=new HColumnDescriptor("Grades");
        //3.1设置版本
        hColumnDescriptor.setVersions(1,5);
        hTableDescriptor.addFamily(hColumnDescriptor);
        //4.修改表信息
        admin.modifyTable(TableName.valueOf("students"),hTableDescriptor);
        //5.释放资源
        HbaseUtils.close(admin);
    }

    /**
     * 删除列簇
     */
    public void deleteColumnFamily() throws IOException {
        //1.获取admin对象
        Admin admin=HbaseUtils.getAdmin();
        //2.直接删除
        admin.deleteColumnFamily(TableName.valueOf("Students"), Bytes.toBytes("StuInfo"));
        //3.释放资源
        HbaseUtils.close(admin);
    }


    /**
     * 查询一个表中所有的列簇信息
     */
    public void listColumnFamily() throws IOException {
        //1.获取admin对象
        Admin admin=HbaseUtils.getAdmin();
        //2.获取表的描述器
        HTableDescriptor hTableDescriptor=admin.getTableDescriptor(TableName.valueOf("students"));
        //3.获取所有的列簇
        HColumnDescriptor[] columnFamilies = hTableDescriptor.getColumnFamilies();
        //4.遍历
        for (HColumnDescriptor columnFamily : columnFamilies) {
            System.out.println(columnFamily.getNameAsString());
            System.out.println(columnFamily.getMaxVersions());
        }
        //5.释放资源
    }

    /**
     * 删除表
     */

    public void dropTable() throws IOException {
        //1.获取admin对象
        Admin admin=HbaseUtils.getAdmin();
        //2.获取表的描述器
        HTableDescriptor hTableDescriptor=admin.getTableDescriptor(TableName.valueOf("students"));
        //3.判断表是否存在
        if(admin.tableExists(TableName.valueOf("students"))){
            //判断表是否有用
            if(admin.isTableEnabled(TableName.valueOf("students"))){
                //弃用表
                admin.disableTable(TableName.valueOf("students"));
            }
            //删除表
            admin.disableTable(TableName.valueOf("students"));
        }
        //4.释放资源
        HbaseUtils.close(admin);
    }

}
