package com.yshanginfo.kettle;



import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
/**
 * <p>Title: java调用kettle4.2数据库型资料库中的转换</p>
 * <p>Description: </p>
 * <p>Copyright: Copyright () 2012</p>
 */
public class ExecuteDataBaseRepTran {
    
    
    private static String transName = "ZFInfoSyncJob";

    public static void main(String[] args) {

        try {
            //初始化kettle环境
            KettleEnvironment.init();
            //创建资源库对象，此时的对象还是一个空对象
            KettleDatabaseRepository repository = new KettleDatabaseRepository();
            //创建资源库数据库对象，类似我们在spoon里面创建资源库
            DatabaseMeta dataMeta = 
            new DatabaseMeta("localhost_myslq","MYSQL","Native","localhost","yshang_kettle_repository","3306","root","123456"); 
            //资源库元对象,名称参数，id参数，描述等可以随便定义
            KettleDatabaseRepositoryMeta kettleDatabaseMeta = 
            new KettleDatabaseRepositoryMeta("yshang_3d", "yshang_3d", "yshang_3d",dataMeta);
            //给资源库赋值
            repository.init(kettleDatabaseMeta);
            //连接资源库
            repository.connect("admin","admin");
            //根据变量查找到模型所在的目录对象,此步骤很重要。
            RepositoryDirectoryInterface directory = repository.findDirectory("/zf");
            JobMeta jobMeta = ((Repository) repository).loadJob(transName, directory, null, null);
            Job job = new Job(repository, jobMeta);   
            job.start();   
            job.waitUntilFinished();   
            if (job.getErrors() > 0) {   
             System.out.println("decompress fail!");   
            }   
            
           /* //创建ktr元对象
            TransMeta transformationMeta = ((Repository) repository).loadTransformation(transName, directory, null, true, null ) ;
            //创建ktr
            Trans trans = new Trans(transformationMeta);
            //执行ktr
            trans.execute(null);
            //等待执行完毕
            trans.waitUntilFinished();
            
            if(trans.getErrors()>0)
            {                   
                System.err.println("Transformation run Failure!");
            }
            else
            {
                System.out.println("Transformation run successfully!");
            }*/
        } catch (KettleException e) {
            e.printStackTrace(); 
        }
    }

}