package com.yshanginfo.kettle;  
  
import java.util.List;  
  
import org.pentaho.di.core.KettleEnvironment;  
import org.pentaho.di.core.exception.KettleException;  
import org.pentaho.di.core.plugins.PluginRegistry;  
import org.pentaho.di.core.plugins.RepositoryPluginType;  
import org.pentaho.di.repository.ObjectId;  
import org.pentaho.di.repository.RepositoriesMeta;  
import org.pentaho.di.repository.Repository;  
import org.pentaho.di.repository.RepositoryElementMetaInterface;  
import org.pentaho.di.repository.RepositoryMeta;  
import org.pentaho.di.repository.StringObjectId;  
import org.pentaho.di.trans.Trans;  
import org.pentaho.di.trans.TransMeta;  
  
/** 
 * 登陆资源库,获取模型,运行 
 *  
 * @author zhangxin 
 *  
 */  
public class TestRepository {  
  
    /** 
     * @param args 
     * @throws KettleException 
     */  
    public static void main(String[] args) throws KettleException {  
        // 运行环境初始化（设置主目录、注册必须的插件等）  
        KettleEnvironment.init();  
        RepositoriesMeta repositoriesMeta = new RepositoriesMeta();  
        // 从文件读取登陆过的资源库信息  
        repositoriesMeta.readData();  
        // 选择登陆过的资源库  
        RepositoryMeta repositoryMeta = repositoriesMeta.findRepository("yshang_3d");  
        // 获得资源库实例  
        Repository repository = PluginRegistry.getInstance().loadClass(RepositoryPluginType.class, repositoryMeta.getId(), Repository.class);  
        repository.init(repositoryMeta);  
        // 连接资源库  
        repository.connect("admin", "admin");  
        ObjectId id = new StringObjectId("0");  
        // 获取某个资源库的所有转换  
        List<RepositoryElementMetaInterface> li = repository.getTransformationObjects(id, false);  
        TransMeta transMeta = null;  
        if (li != null) {  
            for (RepositoryElementMetaInterface repe : li) {  
                System.out.println("TRANSFORMATION:" + repe.getObjectId() + "name:" + repe.getName());  
                TransMeta tm = repository.loadTransformation(repe.getObjectId(), null);  
                if ("测试转换".equals(tm.getName())) {  
                    transMeta = tm;  
                }  
            }  
        }  
        // 执行指定转换  
        Trans trans = new Trans(transMeta);  
        trans.execute(null);  
        trans.waitUntilFinished();  
        if (trans.getErrors() > 0) {  
            throw new RuntimeException("There were errors during transformation execution.");  
        }  
        repository.disconnect();  
    }  
  
}  