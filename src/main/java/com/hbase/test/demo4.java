package com.hbase.test;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * HBase DML操作
 */
public class demo4 {

    /**
     * put操作，插入
     */
    public void put() throws IOException {
        //1.获取Table对象
        Table table = HbaseUtils.getTable();
        //2.创建put对象
        Put put=new Put(Bytes.toBytes("001"));
        //3.指定数据
        put.addColumn(Bytes.toBytes("Grades"),Bytes.toBytes("BigData"),Bytes.toBytes(100));
        //4.提交添加
        table.put(put);
        //5.释放资源
        HbaseUtils.close(table);
    }

    /**
     * 批量插入
     */
    public void batchPut() throws IOException {
        //1.获取Table对象
        Table table = HbaseUtils.getTable();
        //2.创建put对象
        Put put1=new Put(Bytes.toBytes("001"));
        Put put2=new Put(Bytes.toBytes("002"));
        //3.指定数据
        put1.addColumn(Bytes.toBytes("Grades"),Bytes.toBytes("BigData"),Bytes.toBytes(100));
        put2.addColumn(Bytes.toBytes("Grades"), Bytes.toBytes("Computer"), Bytes.toBytes(20));
        //4.提交添加
        List<Put> list=new ArrayList<>();
        list.add(put1);
        list.add(put2);
        table.put(list);
        //5.释放资源
        HbaseUtils.close(table);
    }

    /**
     * 查询操作
     * 查询指定列簇
     */
    public void get() throws IOException {
        //1.获取Table对象
        Table table = HbaseUtils.getTable();
        //2.获取get对象
        Get get=new Get(Bytes.toBytes("003"));
        //3.获取结果对象
        Result result = table.get(get);
        //4.获取到指定列簇
        NavigableMap<byte[], byte[]> stuInfo = result.getFamilyMap(Bytes.toBytes("StuInfo"));
        for (Map.Entry<byte[], byte[]> entry : stuInfo.entrySet()) {
            String k=new String(entry.getKey());
            String v=new String(entry.getValue());
            System.out.println(k+" : "+v);
        }

    }

    /**
     * 查询所有的列簇，列名，列值
     */
    public void get2() throws IOException {
        //1.获取Table对象
        Table table = HbaseUtils.getTable();
        //2.获取get对象
        Get get=new Get(Bytes.toBytes("003"));
        //3.获取结果对象
        Result result = table.get(get);
        //4.获取表格扫描器
        CellScanner cellScanner = result.cellScanner();
        //5.遍历扫描器
        while (cellScanner.advance()){
            //6.获取到一个表格
            Cell cell = cellScanner.current();
            //6.1 列簇
            System.out.println(new String(CellUtil.cloneFamily(cell),"utf-8"));
            //6.1 列名
            System.out.println(new String(CellUtil.cloneQualifier(cell),"utf-8"));
            //6.1 列值
            System.out.println(new String(CellUtil.cloneValue(cell),"utf-8"));
        }
    }

    /**
     * 批量查询
     */
    public void batchGet() throws IOException {
        //1.获取Table对象
        Table table = HbaseUtils.getTable();
        //2.获取get对象
        Get get1=new Get(Bytes.toBytes("001"));
        Get get2=new Get(Bytes.toBytes("002"));
        Get get3=new Get(Bytes.toBytes("003"));
        //3.获取结果数组
        List<Get> list=new ArrayList<>();
        list.add(get1);
        list.add(get2);
        list.add(get3);
        Result[] results = table.get(list);
        //4.遍历
        for (Result result : results) {
            //表格扫描器
            CellScanner cellScanner = result.cellScanner();
            //5.遍历扫描器
            while (cellScanner.advance()){
                //6.获取到一个表格
                Cell cell = cellScanner.current();
                //6.1 列簇
                System.out.println(new String(CellUtil.cloneFamily(cell),"utf-8"));
                //6.1 列名
                System.out.println(new String(CellUtil.cloneQualifier(cell),"utf-8"));
                //6.1 列值
                System.out.println(new String(CellUtil.cloneValue(cell),"utf-8"));
            }
        }
    }

    /**
     * Scan
     */
    public void Scan() throws IOException {
        //1.获取Table对象
        Table table = HbaseUtils.getTable();
        //2.Scan
        Scan scan=new Scan();
        scan.setStartRow(Bytes.toBytes("001"));
        scan.setStopRow(Bytes.toBytes("003"));
        scan.addColumn(Bytes.toBytes("Grades"),Bytes.toBytes("BigData"));
        //3.结果扫描器
        ResultScanner scanner = table.getScanner(scan);
        //4.迭代
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result result = iterator.next();
            HbaseUtils.showResult(result);
        }
    }

    /**
     * 删除操作
     */
    public void delete() throws IOException {
        //1.获取Table对象
        Table table = HbaseUtils.getTable();
        //2.delete
        Delete delete=new Delete(Bytes.toBytes("003"));
        //3.删除
        table.delete(delete);
        //4.释放资源
    }

}
