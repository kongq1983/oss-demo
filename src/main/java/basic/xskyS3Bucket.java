package basic;

import java.util.List;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;

public class xskyS3Bucket 
{
	
	public AmazonS3 client = null;
	
	public xskyS3Bucket(AmazonS3 client) 
	{
		this.client = client;
	}
}
