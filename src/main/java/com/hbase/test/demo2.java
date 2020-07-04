package com.hbase.test;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

import java.io.IOException;

/**
 * NameSpace
 */
public class demo2 {


    /**
     * 创建NameSpace的方法
     */
    @Test
    public void create_nameSpace() throws IOException {
        //1.创建命令空间的描述器对象
        NamespaceDescriptor namespaceDescriptor=NamespaceDescriptor.create("Student").build();
        //2.提交创建
        Admin admin = HbaseUtils.getAdmin();
        admin.createNamespace(namespaceDescriptor);
        //3.关闭连接
        HbaseUtils.close(admin);
    }

    /**
     * 查询所有的NameSpace
     */

    @Test
    public void list_NameSpace() throws IOException {

        //1.获取到Admin对象
        Admin admin = HbaseUtils.getAdmin();
        //2.列举
        NamespaceDescriptor[] namespaceDescriptors=admin.listNamespaceDescriptors();
        //3.遍历
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println(namespaceDescriptor);
        }
        //4.释放资源
        HbaseUtils.close(admin);
        }

    /**
     * 查询指定nameSpace中的所有表（方式1）
     */
    @Test
    public void list_nameSpace_tables_name() throws IOException {
        //1.获取到Admin对象
        Admin admin = HbaseUtils.getAdmin();
        //2.获取到指定的namespace中的所有表名
        TableName[] tableNames = admin.listTableNamesByNamespace("hbase");
        //3.遍历
        for (TableName tableName : tableNames) {
            System.out.println(tableName.getNameAsString());
        }
        //4.关闭资源
        HbaseUtils.close(admin);

    }

    /**
     * 查询指定nameSpace中的所有表（方式2）
     * 这种方式更强大，能拿到更多的东西
     */
    @Test
    public void list_nameSpace_tables_descriptors() throws IOException {
        //1.获取到Admin对象
        Admin admin = HbaseUtils.getAdmin();
        //2.获取到指定的nameSpace中的所有表描述器
        HTableDescriptor[] tableDescriptors = admin.listTableDescriptorsByNamespace("hbase");
        //3.遍历
        for (HTableDescriptor tableDescriptor : tableDescriptors) {
            System.out.println(tableDescriptor.getTableName().getNameAsString());
        }
        //4.释放资源
        HbaseUtils.close(admin);

    }

    /**
     * 删除指定的nameSpace
     */

    public void drop_namespace() throws IOException {
        //1.获取到Admin对象
        Admin admin = HbaseUtils.getAdmin();
        //2.删除
        admin.deleteNamespace("hbase");
        //3.释放资源
        HbaseUtils.close(admin);
    }


}
