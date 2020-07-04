package com.hbase.demo;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class demo2 {

    public static void main(String[] args) {

        SingleColumnValueFilter singleColumnValueFilter=
                new SingleColumnValueFilter(Bytes.toBytes("Grades"),Bytes.toBytes("BigData"),
                        CompareOperator.LESS,Bytes.toBytes("100"));

        ColumnValueFilter columnValueFilter=new ColumnValueFilter(Bytes.toBytes("Grades"),Bytes.toBytes("BigData"),
                CompareOperator.LESS,Bytes.toBytes("100"));



        try {
            testScan(columnValueFilter);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static void testScan(Filter filter) throws Exception {

        Table table = HbaseUtils.getTable();

        Scan scan = new Scan();
        //scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iter = scanner.iterator();
        while (iter.hasNext()) {
            Result result = iter.next();
            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                Cell current = cellScanner.current();
                byte[] familyArray = current.getFamilyArray();
                byte[] valueArray = current.getValueArray();
                byte[] qualifierArray = current.getQualifierArray();
                byte[] rowArray = current.getRowArray();

                System.out.print(new String(rowArray, current.getRowOffset(), current.getRowLength())+"   ");
                System.out.print(new String(familyArray, current.getFamilyOffset(), current.getFamilyLength()));
                System.out.print(":" + new String(qualifierArray, current.getQualifierOffset(), current.getQualifierLength()));
                System.out.println(" " + new String(valueArray, current.getValueOffset(), current.getValueLength()));
            }
            System.out.println("--------------------------------------------------------------------");
        }
    }
}
