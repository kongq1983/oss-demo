package basic;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;

public class xskyS3Object {
	
	public  AmazonS3 client = null;
	
	public xskyS3Object(AmazonS3 client) {
		this.client = client;
	}
	
	// 整体上传
	public PutObjectResult PutObject(String bucketName, String key, File file)
	{
		ObjectMetadata metadata = new ObjectMetadata();
	    metadata.addUserMetadata("Use_test", "test");
		PutObjectRequest putRequest = new PutObjectRequest(bucketName, key, file);
		putRequest.setMetadata(metadata);
		PutObjectResult putResult = null;
		try {
			putResult = client.putObject(putRequest);
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when put object of \""+key+"\".");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		}
		return putResult;
	}
	
	// 通过文件流的方式上传对象
	public PutObjectResult PutObjectRequest(String bucketName, String key, File file)
	{
		InputStream putStream = null;
		PutObjectResult putResult = null;
		try {
			putStream = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.addUserMetadata("Use", "test");
		PutObjectRequest putRequest = new PutObjectRequest(bucketName, key, putStream, metadata);
		try {
			putResult = client.putObject(putRequest);
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when put object of \""+key+"\".");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		}
		return putResult;
	}
	
	// 分段上传
	public String multiPutObject(String bucketName, String key, File file, List<PartETag> partETags)
	{
		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, key);
		InitiateMultipartUploadResult initResult = null;
		
		//创建分段上传
		try {
			initResult = client.initiateMultipartUpload(initRequest);
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when create MultipartUpload of \""+key+"\".");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		}
		
		long fileLength = file.length();
		long partSize = 5 * 1024 * 1024;//5M,分段上传最小分段为5M
		long filePosition = 0;
		UploadPartRequest upRequest = new UploadPartRequest();
		
		upRequest.setBucketName(bucketName);
		upRequest.setKey(key);
		upRequest.setFile(file);
		upRequest.setUploadId(initResult.getUploadId());
		
		//上传文件
		for(int i = 1; filePosition < fileLength; i++){
			partSize = Math.min(partSize, (fileLength - filePosition));
			upRequest.setPartNumber(i);
			upRequest.setFileOffset(filePosition);
			upRequest.setPartSize(partSize);
			try {
				UploadPartResult uploadResult = client.uploadPart(upRequest);
				partETags.add(uploadResult.getPartETag());
			}catch(AmazonServiceException ase) {
				System.out.println("Caught an AmazonServiceException when upload part of \""+key+"\".");
				System.out.println("Error Message:    " + ase.getMessage());
	            System.out.println("HTTP Status Code: " + ase.getStatusCode());
	            System.out.println("AWS Error Code:   " + ase.getErrorCode());
	            System.out.println("Error Type:       " + ase.getErrorType());
	            System.out.println("Request ID:       " + ase.getRequestId());
	            System.exit(1);
			}
			filePosition += partSize;
		}
		return initResult.getUploadId();
	}
	
	//完成分段上传
	public void finish(String bucketName, String key, String uploadId, List<PartETag> partETags) {
		CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags);
		try {
			client.completeMultipartUpload(compRequest);
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when finish MultipartUpload of \""+key+"\".");
			System.out.println("Error Message:    " + ase.getMessage());
        	System.out.println("HTTP Status Code: " + ase.getStatusCode());
        	System.out.println("AWS Error Code:   " + ase.getErrorCode());
        	System.out.println("Error Type:       " + ase.getErrorType());
        	System.out.println("Request ID:       " + ase.getRequestId());
         	System.exit(1);
		}
	}
	
	// 终止分段上传
	public void abortMultiUload(String bucketName, String key, String uploadId) {
		AbortMultipartUploadRequest abortUpload = new AbortMultipartUploadRequest(bucketName, key, uploadId);	
		try {
			client.abortMultipartUpload(abortUpload);
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when abort upload of \""+key+"\".");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		}
	}


	
	// 获取对象
	public S3Object getObject(String bucketName, String key) {
		try {
			return client.getObject(new GetObjectRequest(bucketName, key));
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when get object of \""+key+"\".");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		}
		return null;
	}
	
	// 获取对象元数据
	public ObjectMetadata getMetaData(String bucketName, String key) {
		try {
			return client.getObjectMetadata(bucketName, key);
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when get metadata of \""+key+"\".");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		}
		return null;
	}
	
	// 单个删除对象
	public void DeletingAnObject(String bucketName, String key)
	{
		try {
			client.deleteObject(bucketName, key);	
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when delete object of \""+key+"\".");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		}
	}
	
	// 删除带Version的对象
	public void deleteObjetcWithVersion(String bucketName, String key, String versionId) {
		try {
			client.deleteVersion(bucketName, key, versionId);
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when delete object with version of \""+key+"\".");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		}
	}

	// 批量删除
	public void deleteObjectS(String bucketName, List<DeleteObjectsRequest.KeyVersion> listKeys) {
		DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucketName);
		deleteRequest.withKeys(listKeys);
		try {
			client.deleteObjects(deleteRequest);	
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when delete objects as below:");
			for(DeleteObjectsRequest.KeyVersion key: listKeys) {
				System.out.println("  key: " + key.getKey() + "; Version: " + key.getVersion());	
			}
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		}
	}

	//获取对象预签名URL
	public void getURL(String bucketName, String key, long expTimeMillis, HttpMethod method) {
		
		Date expiration = new java.util.Date();
		expTimeMillis += 1000 * 60 * 60;
		expiration.setTime(expTimeMillis);
		
        // 生成URL
		GeneratePresignedUrlRequest generatePresignedUrlRequest =
			new GeneratePresignedUrlRequest(bucketName, key)
				.withMethod(method)
					.withExpiration(expiration);
		
		URL url = client.generatePresignedUrl(generatePresignedUrlRequest);
		System.out.println("Pre-Signed URL: " + url.toString());
	}
	
	// 复制对象
	public void copyObeject(String sourceBucketName, 
							String sourceKey, 
							String destinationBucketName, 
							String destinationKey) {
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.addUserMetadata("Source", "copy");
		CopyObjectRequest copyReq = new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName, destinationKey)
											.withNewObjectMetadata(metadata);
		try {
			client.copyObject(copyReq);
			System.out.println("Success to copy the object: " + sourceKey);
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when copy object of \""+sourceBucketName+"\".");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		}
	}
	
	// 采用分段上传API进行大对象复制
	public void copyObejectUseMulti(String sourceBucketName, 
									String sourceKey, 
									String destinationBucketName, 
									String destinationKey) {
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.addUserMetadata("Source", "copy");
		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(destinationBucketName, destinationKey);
		initRequest.setObjectMetadata(metadata);
		InitiateMultipartUploadResult initResult = client.initiateMultipartUpload(initRequest);
		
		try {
			GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest(sourceBucketName, sourceKey);
			ObjectMetadata metadataResult =client.getObjectMetadata(metadataRequest);
			long objectSize = metadataResult.getContentLength();
			
			long partSize = 5 * 1024 * 1024;
			long bytePosition = 0;
			int partNum = 1;
			List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
			while (bytePosition < objectSize) {
	            //最后一个part可能比5M小
				long lastByte = Math.min(bytePosition + partSize - 1, objectSize - 1);
				CopyPartRequest copyRequest = new CopyPartRequest()
					.withSourceBucketName(sourceBucketName)
						.withSourceKey(sourceKey)
							.withDestinationBucketName(destinationBucketName)
								.withDestinationKey(destinationKey)
									.withUploadId(initResult.getUploadId())
										.withFirstByte(bytePosition)
											.withLastByte(lastByte)
												.withPartNumber(partNum++);
				copyResponses.add(client.copyPart(copyRequest));
				bytePosition += partSize;
			}
			
			CompleteMultipartUploadRequest completeRequest = new
					CompleteMultipartUploadRequest(destinationBucketName, destinationKey, initResult.getUploadId(), getETags(copyResponses));
			client.completeMultipartUpload(completeRequest);
			
			System.out.println("Multipart copy complete of object:" + sourceKey);
		}catch(AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException when copy object of \""+sourceBucketName+"\".");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		}	
	}
	
	// This is a helper function to construct a list of ETags.
	private static List<PartETag> getETags(List<CopyPartResult> responses) {
		List<PartETag> etags = new ArrayList<PartETag>();
		for (CopyPartResult response : responses) {
			etags.add(new PartETag(response.getPartNumber(), response.getETag()));
		}	
		return etags;
	}
	
	public void listversion() {
		String bucketName="liudonghai";
        VersionListing versionList = client.listVersions(new ListVersionsRequest()
        													.withBucketName(bucketName)
        														.withMaxResults(1)
        															.withDelimiter("\\"));
        while (true) {
            Iterator<S3VersionSummary> versionIter = versionList.getVersionSummaries().iterator();
            while (versionIter.hasNext()) {
                S3VersionSummary vs = versionIter.next();
                //client.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
                System.out.println(vs.getVersionId());
            }

            if (versionList.isTruncated()) {
                versionList = client.listNextBatchOfVersions(versionList);
            } else {
                break;
            }
        }
	}
}
