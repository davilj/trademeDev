package trademe.model.processors;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import trademe.model.Listing;

public class ListingPresisterImpl implements IProcessor<Listing> {
	AmazonS3 s3Client;
	
	
	public ListingPresisterImpl() {
		BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAJGTV3Z3HCP4PX33Q", "tbGx87RJ7uyol4LBumNaUwdFJDwqUSLufPVltx4u");
		s3Client = new AmazonS3Client(awsCreds);
	}

	@Override
	public void process(Listing listing) {
		if (listing.doWeHaveAbid()) {
			//download extra info
		}
		
		//persist to s3
		
		
	}

}
