package org.geekbang.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author chen.xinliang
 * @create 2022-03-19 16:11
 */
public class APIDemo {
    private static Connection connection=null;
    private static Admin admin=null;

    static {

        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","ck01,ck02,ck03");
            connection=ConnectionFactory.createConnection(configuration);
            admin=connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        createNameSpace("chenxinliang");
        createTable("chenxinliang:student","name","info","score");
        putData("chenxinliang:student","1001","name","","Tom");
        putData("chenxinliang:student","1001","info","student_id","20210001");
        putData("chenxinliang:student","1001","info","class","1");
        putData("chenxinliang:student","1001","score","understanding","75");
        putData("chenxinliang:student","1001","score","programming","82");
        getData("chenxinliang:student","1001","name","");
        System.out.println("********************************");
        scanTable("chenxinliang:student");
        deleteData("chenxinliang:student","1001","info","class");
    }

    public static boolean isTableExist(String tableName) throws IOException {
        boolean exists =admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }

    public static void createTable(String tableName,String... cfs) throws IOException {
        if(cfs.length <=0){
            System.out.println("Please input column family!");
            return;
        }
        if(isTableExist(tableName)){
            System.out.println(tableName+" table exist!");
            return;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
    }

    public static void dropTable(String tableName) throws IOException {
        if(!isTableExist(tableName)){
            System.out.println(tableName +" table not exist!");
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));


    }

    public static void createNameSpace(String ns){
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        }catch (NamespaceExistException e){
            System.out.println(ns+" namespace exist!");
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
//        get.addFamily(Bytes.toBytes(cf));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        get.setMaxVersions();

        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                    +",CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                    ",Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }
    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                        +",CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                        ",Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }
    public static void deleteData(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));
        table.delete(delete);
        table.close();

    }
    public static void close(){
        if (admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
