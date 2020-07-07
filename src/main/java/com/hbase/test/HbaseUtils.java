package com.hbase.test;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.Iterator;

/**
 * HBase 工具类
 */
public class HbaseUtils {
    private final static String HBASE_KEY="hbase.zookeeper.quorum";
    private final static String HBASE_VALUE="hbase1,hbase2,hbase3";

    private static Connection connection;

    static {
        try{
            //1.获取配置对象
            Configuration configuration = HBaseConfiguration.create();
            //1.1设置连接hbase的参数
            configuration.set(HBASE_KEY,HBASE_KEY);
            //2.获取到连接对象
            connection= ConnectionFactory.createConnection(configuration);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static Admin getAdmin(){
        try{
            //获取核心对象Admin
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
