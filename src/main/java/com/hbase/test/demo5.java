package com.hbase.test;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 过滤器查询
 */
public class demo5 {

    /**
     * 单列值过滤器 SingleColumnValueFilter
     * select * from t1 where age<18
     */
    public void singleColumnValueFilter(){
        //1.创建单列值过滤器
        SingleColumnValueFilter singleColumnValueFilter=
                new SingleColumnValueFilter(Bytes.toBytes("f1"),Bytes.toBytes("age"),
                        CompareFilter.CompareOp.LESS,Bytes.toBytes("18"));
        //1.1设置单值过滤，如果没有这个属性列，就不计算他
        singleColumnValueFilter.setFilterIfMissing(true);
        //2.创建Scan
        Scan scan=new Scan();
        //3.设置过滤器
        scan.setFilter(singleColumnValueFilter);
        //4.获取表
        Table table = HbaseUtils.getTable();
        //5.打印
        HbaseUtils.showScan(table,scan);
        //6.释放资源
    }

    /**
     * 过滤器链
     * select * from t1 where age<18 and name=lixi
     */
    public void filterList(){
        //1.创建过滤器链
        FilterList filterList=new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //2.创建单列值过滤器
        SingleColumnValueFilter singleColumnValueFilter1=
                new SingleColumnValueFilter(Bytes.toBytes("f1"),Bytes.toBytes("age"),
                        CompareFilter.CompareOp.LESS,Bytes.toBytes("18"));
        SingleColumnValueFilter singleColumnValueFilter2=
                new SingleColumnValueFilter(Bytes.toBytes("f1"),Bytes.toBytes("name"),
                        CompareFilter.CompareOp.EQUAL,Bytes.toBytes("lixi"));
        //2.1设置单值过滤，如果没有这个属性列，就不计算他
        singleColumnValueFilter1.setFilterIfMissing(true);
        singleColumnValueFilter2.setFilterIfMissing(true);
        //2.2添加到过滤器链中
        filterList.addFilter(singleColumnValueFilter1);
        filterList.addFilter(singleColumnValueFilter2);
        //3.创建Scan
        Scan scan=new Scan();
        //4.设置过滤器
        scan.setFilter(filterList);
        //5.获取表
        Table table = HbaseUtils.getTable();
        //6.打印
        HbaseUtils.showScan(table,scan);
        //7.释放资源
    }

    /**
     * SubStringComparator 子串比较器
     */


    /**
     * 正则比较器 RegexStringComparator
     *
     * select * from t1 where name like %X%
     */
    public void regexStringComparator(){
        //0.创建正则比较器
        RegexStringComparator regexStringComparator=
                new RegexStringComparator("\\W*X\\W*");
        //1.创建单列值过滤器
        SingleColumnValueFilter singleColumnValueFilter=
                new SingleColumnValueFilter(Bytes.toBytes("f1"),Bytes.toBytes("age"),
                        CompareFilter.CompareOp.EQUAL,regexStringComparator);
        //1.1设置单值过滤，如果没有这个属性列，就不计算他
        singleColumnValueFilter.setFilterIfMissing(true);
        //2.创建Scan
        Scan scan=new Scan();
        //3.设置过滤器
        scan.setFilter(singleColumnValueFilter);
        //4.获取表
        Table table = HbaseUtils.getTable();
        //5.打印
        HbaseUtils.showScan(table,scan);
        //6.释放资源
    }


    /**
     *  BinaryComparator 二进制比较器（过滤器查询中默认使用的比较器）
     *  select * from t1 where name=lixi
     */

    public void binareComparator(){
        //1.创建比较器
        BinaryComparator binaryComparator=new BinaryComparator(Bytes.toBytes("lixi"));
        //2.创建单列值过滤器
        SingleColumnValueFilter singleColumnValueFilter=
                new SingleColumnValueFilter(Bytes.toBytes("f1"),Bytes.toBytes("name"),
                        CompareFilter.CompareOp.EQUAL,binaryComparator);
        //2.1设置单值过滤，如果没有这个属性列，就不计算他
        singleColumnValueFilter.setFilterIfMissing(true);
        //3.打印
        HbaseUtils.showFilter(singleColumnValueFilter);
    }

    /**
     * 二进制前缀比较器 BinaryprefixComparator
     * select * from t1 where name=lixi
     */


    /**
     * 列簇过滤器 FamilyFilter
     */
    public void familyFilter1(){
        //0.创建正则比较器
        RegexStringComparator regexStringComparator=
                new RegexStringComparator("\\W*f\\*");
        //创建列簇过滤器
        FamilyFilter familyFilter=
                new FamilyFilter(CompareFilter.CompareOp.EQUAL,regexStringComparator);
        //打印数据
        HbaseUtils.showFilter(familyFilter);
    }

    public void familyFilter2(){
        //创建前缀比较器
        SubstringComparator substringComparator=new SubstringComparator("f");
        //创建列簇过滤器
        FamilyFilter familyFilter=
                new FamilyFilter(CompareFilter.CompareOp.EQUAL,substringComparator);
        //打印数据
        HbaseUtils.showFilter(familyFilter);
    }


    /**
     * 列名过滤器 QualifterFilter
     */
    public void qualifterFilter(){
        //创建前缀比较器
        SubstringComparator substringComparator=new SubstringComparator("ge");
        //创建列名过滤器
       QualifierFilter qualifierFilter=
               new QualifierFilter(CompareFilter.CompareOp.EQUAL,substringComparator);
        //打印数据
        HbaseUtils.showFilter(qualifierFilter);
    }

    /**
     * 列名前缀过滤器 columnPrefixFilter
     */
    public void columnPrefexFilter(){
        ColumnPrefixFilter columnPrefixFilter=new ColumnPrefixFilter(Bytes.toBytes("a"));
        HbaseUtils.showFilter(columnPrefixFilter);
    }

    /**
     * 多列值前缀过滤器 mulipleColumnPrefixFilter
     */
    public void mulipleColumnPrefixFilter(){
        byte[][] prefexs={Bytes.toBytes("a"),Bytes.toBytes("n")};
        MultipleColumnPrefixFilter multipleColumnPrefixFilter=
                new MultipleColumnPrefixFilter(prefexs);
        HbaseUtils.showFilter(multipleColumnPrefixFilter);
    }

    /**
     * 列范围过滤器ColumnRangleFilter
     * True就是把当前值包含上，false是不把当前值包含上
     */
    public void columnRangleFilter(){
        ColumnRangeFilter columnRangeFilter=
                new ColumnRangeFilter(Bytes.toBytes("age"),true,Bytes.toBytes("name"),true);
        HbaseUtils.showFilter(columnRangeFilter);
    }


    /**
     * 行键过滤器 RowFilter
     */
    public void rowFilter(){
        BinaryComparator binaryComparator=new BinaryComparator(Bytes.toBytes("001"));
        RowFilter rowFilter=new RowFilter(CompareFilter.CompareOp.EQUAL,binaryComparator);
        HbaseUtils.showFilter(rowFilter);
    }

    /**
     * FirstKeyOnlyFilter首列的键的过滤器
     */
    public void firstKeyOnlyFiler(){
        FirstKeyOnlyFilter firstKeyOnlyFilter=new FirstKeyOnlyFilter();
        HbaseUtils.showFilter(firstKeyOnlyFilter);
    }

}
