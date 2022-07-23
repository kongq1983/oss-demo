package demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
//import org.apache.log4j.PropertyConfigurator;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import basic.xskyS3Basic;
import basic.xskyS3Bucket;
import basic.xskyS3Object;

public class xskyS3Demo 
{
    private static String bucketName = null;
    private static String MultipartUploadFile = null;
    private static String UploadFile = null;
    private static String endpoint = "10.252.90.20";
    public xskyS3Basic basic = null;
    public xskyS3Bucket S3Bucket = null;
    public xskyS3Object S3Object = null;
    
    public static void main(String[] args) 
    {
		getConf(args[0]);
		xskyS3Demo demo = new xskyS3Demo();
		demo.show();

    }
    
    public void show() {
    	String uploadIdforlistParts = null;
        try {	
            
            // 整体上传
            System.out.println(new Date().toString());
            System.out.println("Begin to put object \"" + UploadFile + "\" by putObject.");
            File file1 = new File(UploadFile);
            S3Object.PutObject(bucketName, UploadFile, file1);
            System.out.println("Successful put object \"" + UploadFile + "\" by putObject.\n");
            System.out.println(new Date().toString());
            
            // 分段上传
            System.out.println("Begin to put object \"" + MultipartUploadFile + "\" by MultipartUpload.");
            File file2 = new File(MultipartUploadFile);
            List<PartETag> partETags=new ArrayList<PartETag>();
            uploadIdforlistParts = S3Object.multiPutObject(bucketName, MultipartUploadFile, file2, partETags);
            System.out.println("Successful put object \"" + MultipartUploadFile + "\" by MultipartUpload.\n");

    
            //完成分段上传
            S3Object.finish(bucketName, MultipartUploadFile, uploadIdforlistParts, partETags);
            System.out.println();
            

            System.out.println();
            
            // 读取对象
            String getFile = UploadFile + "_new";
            S3Object s3object = S3Object.getObject(bucketName, UploadFile);
            System.out.println("Get object: " + UploadFile + " as " + getFile);
            xskyS3Basic.WriteNewFile(s3object.getObjectContent(), getFile); 
            s3object.close();
			
			// 预签名
			java.util.Date expiration = new java.util.Date(); 
            long expTimeMillis = expiration.getTime(); 
            System.out.println(expTimeMillis);
            S3Object.getURL(bucketName, UploadFile, expTimeMillis, HttpMethod.GET);
			
            // 删除对象
        	System.out.println("Begin delete objects: " + UploadFile + ", " + MultipartUploadFile + ".\n");
        	List<DeleteObjectsRequest.KeyVersion> listKeys = new ArrayList<DeleteObjectsRequest.KeyVersion>();
        	listKeys.add(new DeleteObjectsRequest.KeyVersion(UploadFile));
        	listKeys.add(new DeleteObjectsRequest.KeyVersion(MultipartUploadFile));
        	S3Object.deleteObjectS(bucketName, listKeys);
			
        }  catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which" + " means your request made it "
                    + "to Amazon S3, but was rejected with an error response" + " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means" + " the client encountered "
                    + "an internal error while trying to " + "communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }catch (IllegalArgumentException  e) {
        	System.out.println("Caught an IllegalArgumentException, Maybe you input a wrong argument.");
        	System.out.println("Error Message:	" + e.getMessage());	
        }catch (IOException e) {
        	System.out.println("Error Message:	" + e.getMessage());	
        }
    }
    
    public xskyS3Demo() {
    	//加载日志配置文件
//    	PropertyConfigurator.configure(".\\src\\log4j.properties");
        
    	// 创建连接
    	basic = new xskyS3Basic(endpoint);
        S3Bucket = new xskyS3Bucket(basic.client);
        S3Object = new xskyS3Object(basic.client);
    }

    public static void getConf(String fileName) {
    	try {
    		FileReader reader = new FileReader(fileName);
			BufferedReader br = new BufferedReader(reader);
            String str = null;                      
            while((str = br.readLine()) != null) {                               
            	String[] conf = str.split("=", 2);
            	if(conf[0].trim().compareTo("Endpoint") == 0) {
                    endpoint = conf[1].trim();
                }
            	if(conf[0].trim().compareTo("BucketName") == 0) {
                    bucketName = conf[1].trim();
                }
            	if(conf[0].trim().compareTo("MultipartUploadFile") == 0) {
                    MultipartUploadFile = conf[1].trim();
                }
            	if(conf[0].trim().compareTo("UploadFile") == 0) {
                    UploadFile = conf[1].trim();
                }
            }
            reader.close();
            br.close();
        } catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}catch(IOException e) {
            e.printStackTrace();
            System.exit(1);
      }
    }

}