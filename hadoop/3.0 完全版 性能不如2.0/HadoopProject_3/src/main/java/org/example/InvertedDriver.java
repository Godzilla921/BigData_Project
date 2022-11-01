package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class InvertedDriver {
    private static Configuration configuration = null;
    private static Connection connection = null;
    // 倒排索引Hbase 表只有唯一的列族 File
    public static ArrayList<String> columnFamily=null;
    public static  String tableName = "";
    public static  String nameSpace = "";
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 2) {
            System.err.println("语法: Inverted Index <inPath> <namespace:table>");
            System.exit(1);
        }
        // 初始化连接操作
        init(args[1]);
        // 初始化建表操作
        initHbase();
        Job job = Job.getInstance(configuration, "Inverted Index");
        job.setJarByClass(InvertedDriver.class);

        job.setMapperClass(InvertedMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(InvertedReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob(nameSpace+":"+tableName, InvertedReducer.class,job);
        job.setCombinerClass(InvertedCombiner.class);
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
        //创建列族
        columnFamily=new ArrayList<String>();
        columnFamily.add("File");
    }
    public static void initHbase() throws IOException {
        //创建命名空间
        createNameSpace(nameSpace);
        //创建表格
        createTable(nameSpace, tableName,columnFamily);
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
}

