package basic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;


public class xskyS3Basic {
	
	public  AmazonS3 client = null;
	
	//创建连接
	public xskyS3Basic(String serverUrl) {
		
		AWSCredentials credentials = null;
        try {
        	credentials = new ProfileCredentialsProvider().getCredentials();	
    		//credentials = new EnvironmentVariableCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.", e);
        }
        
        //初始化S3 configure 的实例
        ClientConfiguration config = new ClientConfiguration();
        								
        config.setProtocol(Protocol.HTTP);        
        config.setUseExpectContinue(false);
        EndpointConfiguration end_point = null;
        end_point = new AwsClientBuilder.EndpointConfiguration(serverUrl, "us-east-1");
        
        //创建连接,替换原AmazonS3Client接口
        client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                     .withClientConfiguration(config)
                         .withEndpointConfiguration(end_point)
                         	.withPathStyleAccessEnabled(true)
                         		.build();
	}
	
	
    public static void WriteNewFile(InputStream input, String fileName) throws IOException 
    {
        FileOutputStream fos = new FileOutputStream(new File(fileName));
        //一次性取多少字节
        byte[] bytes = new byte[128];
        int n = -1;
        while ((n = input.read(bytes, 0, bytes.length)) != -1) {
        	fos.write(bytes, 0, n);
        }
        fos.close();
    }
	
}
