package demo;

import com.amazonaws.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * @author kq
 * @date 2022-07-23 13:39
 * @since 2020-0630
 */
public class AWSMyDemo {

    private AmazonS3 s3client;

    //accessKey:用户的Access Key ID
    //secretKey:用户的Access Key Secret
    //hostname:MSS的endpoint服务地址
    public AmazonS3 createAmazonS3Conn (String accessKey, String secretKey, String endpoint){
        // //获取访问凭证
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        //设置连接时的参数
        ClientConfiguration config = new ClientConfiguration();
        //设置连接方式为HTTP，可选参数为HTTP和HTTPS
        config.setProtocol(Protocol.HTTP);
        //config.setProtocol(Protocol.HTTPS);
        //设置网络访问超时时间
        config.setConnectionTimeout(30000);
        config.setUseExpectContinue(true);

        //设置Endpoint
        AwsClientBuilder.EndpointConfiguration end_point = null;
        end_point = new AwsClientBuilder.EndpointConfiguration(endpoint, "");
        //创建连接
        AmazonS3 s3client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(config).withEndpointConfiguration(end_point)
                        .withPathStyleAccessEnabled(true).build();

        this.s3client = s3client;

        return s3client;
    }

    /**
     * 上传
     * @param bucketName
     * @param objectName
     * @param in
     */
    public void putObject(String bucketName, String objectName, InputStream in){
        try{
            //bucketName指定上传文件所在的桶名
            //objectName指定上传的文件名
            //in指定上传的文件流

            //创建用户自定义元数据
//            ObjectMetadata metadata = new ObjectMetadata();
//            metadata.addUserMetadata("Use", "test");

            String key = String.valueOf(System.currentTimeMillis());

            // 创建上传文件请求（PutObjectRequest）
            PutObjectRequest putRequest = new PutObjectRequest(bucketName, key, in, null);

            System.out.println("putRequest="+putRequest);

        }catch (AmazonServiceException ase) {
            //存储服务端处理异常
            System.out.println("Caught an ServiceException.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        }catch (AmazonClientException ace) {
            //客户端处理异常
            System.out.println("Caught an ClientException.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }


    //获取对象预签名URL
    public void getURL(String bucketName, String objectName) {
        try{
            //设定url的有效时间
            java.util.Date expiration = new java.util.Date();
            long milliSeconds = expiration.getTime();
            milliSeconds += 1000 * 60 * 60; // Add 1 hour.
            expiration.setTime(milliSeconds);

            //指定授权的bucket和object
            GeneratePresignedUrlRequest generatePresignedUrlRequest =
                    new GeneratePresignedUrlRequest(bucketName, objectName);
            //指定授权的请求类型
            generatePresignedUrlRequest.setMethod(HttpMethod.GET);
            generatePresignedUrlRequest.setExpiration(expiration);

            //生成授权的url
            URL url = s3client.generatePresignedUrl(generatePresignedUrlRequest);
            System.out.println("Pre-Signed URL = " + url.toString());

        }catch (AmazonServiceException ase) {
            //存储服务端处理异常
            System.out.println("Caught an ServiceException.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        }catch (AmazonClientException ace) {
            //客户端处理异常
            System.out.println("Caught an ClientException.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }


    public static void main(String[] args) throws IOException {
        String accessKey = "";
        String secretKey = "";
        String endpoint = "";
        AWSMyDemo demo = new AWSMyDemo();
        demo.createAmazonS3Conn(accessKey,secretKey,endpoint);

        String bucketName = "";
        String objectName = String.valueOf(System.currentTimeMillis());
        System.out.println("bucketName="+bucketName);
        System.out.println("objectName="+objectName);

        String fileName = "";
        try(InputStream in = new FileInputStream(fileName)) {
            demo.putObject(bucketName,objectName,in);
        }


        demo.getURL(bucketName,objectName);

    }

}
