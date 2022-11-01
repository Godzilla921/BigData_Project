package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

        public class InvertedDriver {
    private static Configuration configuration = null;
    private static Connection connection = null;
    // 倒排索引Hbase 表只有唯一的列族 File
    public static ArrayList<String> columnFamily=null;
    public static  String tableName = "";
    public static  String nameSpace = "";
    public InvertedDriver() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("语法: Inverted Index <inPath> <namespace:table> <output>");
            System.exit(1);
        }
        // 初始化连接操作
        init(args[1]);
        // 初始化建表操作
        initHbase();

        Job job = Job.getInstance(configuration, "Inverted Index");

        job.setJarByClass(InvertedDriver.class);
        job.setMapperClass(InvertedMapper.class);
        job.setCombinerClass(InvertedCombiner.class);
        job.setReducerClass(InvertedReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static void init(String namespace_tableName) throws IOException {
        // 得到命名空间和表名
        String[] strings=namespace_tableName.split(":");
        if(strings.length<2){
            System.err.println("语法: namespace:tableName");
            System.exit(1);
        }
        // 得到命名空间 和 表名
        nameSpace=strings[0];
        tableName=strings[1];
        // 初始化连接操作
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://master:9000/hbase");
        connection = ConnectionFactory.createConnection(configuration);
        columnFamily=new ArrayList<String>();
        columnFamily.add("File");
    }
    public static void initHbase() throws IOException {
        //创建命名空间
        createNameSpace(nameSpace);
        //创建表格
        createTable(nameSpace, tableName,columnFamily);
        // 插入数据测试
//        test_Hbase(nameSpace,tableName);
    }

    public static void createTable(String namespace,String tableName,ArrayList<String> columnFamilies) throws IOException {
        Admin admin = connection.getAdmin();
        if (isExist(namespace,tableName)){
            System.out.println("table:" + namespace+": "+tableName + " is existed.");
        }else{
            TableDescriptorBuilder tableDescriptorBuilder=TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(namespace,tableName));
            for(String column:columnFamilies){
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder
                        = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column));
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("table: "  + namespace+": "+tableName + " create success.");
        }
        admin.close();
    }
    // 创建命名空间
    public static void createNameSpace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        //判断该命名空间是否被创建
        String[] listNamespaces = admin.listNamespaces();
        for (String ns:listNamespaces
             ) {
            if (ns.equals(namespace)){
                System.out.println(namespace+" is existed!");
                admin.close();
                return;
            }
        }// 创建命名空间
        NamespaceDescriptor.Builder builder=NamespaceDescriptor.create(namespace);
        admin.createNamespace(builder.build());
        admin.close();
    }
    // 判断表是否存在
    public static boolean isExist(String nameSpace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        boolean flag=admin.tableExists(TableName.valueOf(nameSpace,tableName));
        admin.close();
        return  flag;
    }
    // 插入一条数据
    public static void insert(String nameSpace,String tableName,String rowKey,String columnFamily,
                              String column,String value) throws IOException {
        Table table=connection.getTable(TableName.valueOf(nameSpace,tableName));
        // 设置行键
        Put put=new Put(rowKey.getBytes());
        // 列族  列  数据
        put.addColumn(columnFamily.getBytes(),column.getBytes(),value.getBytes());
        table.put(put);
        table.close();
    }
    // 插入一群数据
    public static void insert(String nameSpace,String tableName,ArrayList<String> rowKeys,ArrayList<String> columnFamilies,
                              ArrayList<String> columns,ArrayList<String> values) throws IOException {
        Table table=connection.getTable(TableName.valueOf(nameSpace,tableName));
        // 设置行键
        int len =rowKeys.size();
        for(int i=0;i<len;++i){
            Put put=new Put(rowKeys.get(i).getBytes());
            // 列族  列  数据
            put.addColumn(columnFamilies.get(i).getBytes(),columns.get(i).getBytes(),values.get(i).getBytes());
            table.put(put);
        }
        table.close();
    }
    // 一个rowKey 插入多列数据
    public static void insert(String rowKey,ArrayList<String> columns,ArrayList<String> values)
            throws IOException {
        Table table=connection.getTable(TableName.valueOf(nameSpace,tableName));
        // 设置行键
        int len =columns.size();
        Put put=new Put(rowKey.getBytes());
        for(int i=0;i<len;++i){
            // 列族  列  数据
            put.addColumn(columnFamily.get(0).getBytes(),columns.get(i).getBytes(),values.get(i).getBytes());
            table.put(put);
        }
        table.close();
    }
    private static void test_Hbase(String nameSpace,String tableName) throws IOException {
        // 测试向Hbase数据库的数据
        insert(nameSpace,tableName,"mapper",columnFamily.get(0),"file_02","3");
        insert(nameSpace,tableName,"mapper",columnFamily.get(0),"file_01","4");
        insert(nameSpace,tableName,"is",columnFamily.get(0),"file_02","1");
        insert(nameSpace,tableName,"is",columnFamily.get(0),"file_04","1");
        insert(nameSpace,tableName,"a",columnFamily.get(0),"file_02","7");
        insert(nameSpace,tableName,"apple",columnFamily.get(0),"file_03","2");
        insert(nameSpace,tableName,"mapper",columnFamily.get(0),"file_00","4");
        insert(nameSpace,tableName,"hello",columnFamily.get(0),"file_02","5");
        insert(nameSpace,tableName,"Jack",columnFamily.get(0),"file_02","8");
        insert(nameSpace,tableName,"mapper",columnFamily.get(0),"file_04","9");
    }
}

