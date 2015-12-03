package com.yshanginfo.kettle;
import java.io.File;
import java.util.List;

import org.pentaho.di.core.KettleEnvironment; 
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.job.Job; 
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.trans.Trans; 
import org.pentaho.di.trans.TransMeta; 
 
public class Transformation 
{ 
    public void runTran(String filename) {   
        try {   
			KettleEnvironment.init();   
            TransMeta transMeta = new TransMeta(filename);   
            Trans trans = new Trans(transMeta);   
            //此处为输入的参数，也可以通过参数传进方法中 
            String [] s = {"我是参数1","我是参数2"}; 
            trans.prepareExecution(s);   
            trans.startThreads();   
            trans.waitUntilFinished();   
            if (trans.getErrors() != 0) {   
                System.out.println("Error");   
            }   
        } catch (KettleException e) {   
            e.printStackTrace();   
        }   
    }   
     
    public void runJob(String jobname){   
          try {   
           KettleEnvironment.init();   
           //jobname 是Job脚本的路径及名称   
           JobMeta jobMeta = new JobMeta(jobname, null);
           List<JobEntryCopy> jobCopies = jobMeta.getJobCopies();
           System.out.println("-------"+jobCopies.size()+"---------");
           JobEntryCopy jobEntryCopy = jobCopies.get(1);
           System.out.println(jobEntryCopy.getName());
           
           Job job = new Job(null, jobMeta);   
           String temp = Transformation.class.getClassLoader().getResource("").getPath() + "etl/test.ktr";
           //向Job 脚本传递参数，脚本中获取参数值：${参数名}   
           //job.setVariable(paraname, paravalue);   
           job.setVariable("filename", temp);
           job.start();   
           job.waitUntilFinished();   
           if (job.getErrors() > 0) {   
            System.out.println("decompress fail!");   
           }   
          } catch (KettleException e) {   
           System.out.println(e);   
          }   
     } 
     
    public static void main(String[] args) 
    { 
    	//System.out.println(System.getProperty("user.dir"));
    	String temp = Transformation.class.getClassLoader().getResource("").getPath() + "etl/test.ktr";
    	System.out.println(temp);
        Transformation transformation = new Transformation(); 
        
        transformation.runTran(temp); 
//        transformation.runJob(Transformation.class.getClassLoader().getResource("").getPath() + "etl/test.kjb"); 
//        transformation.runTran(System.getProperty("user.dir")+"\\src\\转换1.ktr"); 
       // transformation.runJob("C:\\Users\\Administrator\\Desktop\\SendMail.kjb"); 
         
        /* 控制台输出 
            INFO  22-03 21:38:43,981 - 转换1 - 为了转换解除补丁开始  [转换1] 
            INFO  22-03 21:38:44,002 - 获取系统信息 - 完成处理 (I=0, O=0, R=1, W=1, U=0, E=0 
            INFO  22-03 21:38:44,007 - 文本文件输出 - 完成处理 (I=0, O=2, R=1, W=1, U=0, E=0 
            INFO  22-03 21:38:44,044 - SendMail - 开始执行任务 
            INFO  22-03 21:38:44,048 - SendMail - 开始项[Mail] 
            INFO  22-03 21:38:46,890 - SendMail - 完成作业项[Mail] (结果=[true]) 
            INFO  22-03 21:38:46,890 - SendMail - 任务执行完毕 
         */ 
    } 
} 