package com.hbase.demo;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Scanner;

public class HbaseUtils {
    private final static String HBASE_ZNODE_PARENT="/hbase223";
    private final static String HBASE_ZK_QUORUM="192.168.125.22";
    private final static String HBASE_CLIENTPORT="2181";

    private static Connection connection;

    static {
        try{
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("zookeeper.znode.parent",HBASE_ZNODE_PARENT); //与 hbase-site-xml里面的配置信息 zookeeper.znode.parent 一致
            configuration.set("hbase.zookeeper.quorum",HBASE_ZK_QUORUM);  //hbase 服务地址
            configuration.set("hbase.zookeeper.property.clientPort",HBASE_CLIENTPORT); //端口号
            connection= ConnectionFactory.createConnection(configuration);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static Admin getAdmin(){
        try{
            HBaseAdmin hBaseAdmin =(HBaseAdmin) connection.getAdmin();
            return hBaseAdmin;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static void close(Admin admin){
        if(admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static Table getTable(){
        return getTable("Students");
    }

    public static Table getTable(String tableName){
        Table table=null;

        try {
            if(StringUtils.isEmpty(tableName)){ return null;}
            table =connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    public static void close(Table table){
        if (table!=null){
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



    public static void showResult(Result result){
        CellScanner cellScanner=result.cellScanner();
        try{
            while(cellScanner.advance()){
                Cell cell=cellScanner.current();
                System.out.println(new String(CellUtil.cloneFamily(cell),"utf-8"));
                System.out.println(new String(CellUtil.cloneQualifier(cell),"utf-8"));
                System.out.println(new String(CellUtil.cloneValue(cell),"utf-8"));

            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void showScan(Table table,Scan scan){
        try {
            ResultScanner scanner=table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()){
                Result result=iterator.next();
                HbaseUtils.showResult(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void showFilter(Filter filter){
        Scan scan=new Scan();
        scan.setFilter(filter);
        Table table = HbaseUtils.getTable();
        HbaseUtils.showScan(table,scan);
        HbaseUtils.close(table);
    }
}
