package com.kq.aliyun.oss.demo;

/**
 * @author kq
 * @date 2022-07-23 15:23
 * @since 2020-0630
 */
import java.io.File;
import java.io.IOException;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;

/**
 * https://github.com/aliyun/aliyun-oss-java-sdk/blob/master/src/samples/ImageSample.java?spm=a2c4g.11186623.0.0.8a0746a1aqv97V&file=ImageSample.java
 * Image process examples.
 *
 */
public class ImageSample {

    private static String endpoint = "<endpoint, http://oss-cn-hangzhou.aliyuncs.com>";
    private static String accessKeyId = "<accessKeyId>";
    private static String accessKeySecret = "<accessKeySecret>";
    private static String bucketName = "<bucketName>";
    private static String key = "example.jpg";


    public static void main(String[] args) throws IOException {

        OSS ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);

        try {
            // resize
            String style = "image/resize,m_fixed,w_100,h_100";
            GetObjectRequest request = new GetObjectRequest(bucketName, key);
            request.setProcess(style);

            ossClient.getObject(request, new File("example-resize.jpg"));

            // crop
            style = "image/crop,w_100,h_100,x_100,y_100,r_1";
            request = new GetObjectRequest(bucketName, key);
            request.setProcess(style);

            ossClient.getObject(request, new File("example-crop.jpg"));

            // rotate
            style = "image/rotate,90";
            request = new GetObjectRequest(bucketName, key);
            request.setProcess(style);

            ossClient.getObject(request, new File("example-rotate.jpg"));

            // sharpen
            style = "image/sharpen,100";
            request = new GetObjectRequest(bucketName, key);
            request.setProcess(style);

            ossClient.getObject(request, new File("example-sharpen.jpg"));

            // add watermark into the image
            style = "image/watermark,text_SGVsbG8g5Zu-54mH5pyN5YqhIQ";
            request = new GetObjectRequest(bucketName, key);
            request.setProcess(style);

            ossClient.getObject(request, new File("example-watermark.jpg"));

            // convert format
            style = "image/format,png";
            request = new GetObjectRequest(bucketName, key);
            request.setProcess(style);

            ossClient.getObject(request, new File("example-format.png"));

            // image information
            style = "image/info";
            request = new GetObjectRequest(bucketName, key);
            request.setProcess(style);

            ossClient.getObject(request, new File("example-info.txt"));

        } catch (OSSException oe) {
            System.out.println("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason.");
            System.out.println("Error Message: " + oe.getErrorMessage());
            System.out.println("Error Code:       " + oe.getErrorCode());
            System.out.println("Request ID:      " + oe.getRequestId());
            System.out.println("Host ID:           " + oe.getHostId());
        } catch (ClientException ce) {
            System.out.println("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ce.getMessage());
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            ossClient.shutdown();
        }
    }
}