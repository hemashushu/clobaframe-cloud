package org.archboy.clobaframe.blobstore.amazons3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import javax.inject.Inject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.archboy.clobaframe.blobstore.BlobResourceInfo;
import org.archboy.clobaframe.blobstore.BlobResourceInfoFactory;
import org.archboy.clobaframe.blobstore.BlobResourceInfoPartialCollection;
import org.archboy.clobaframe.blobstore.BlobKey;
import org.archboy.clobaframe.blobstore.Blobstore;
import static org.junit.Assert.*;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/applicationContext.xml" })
public class S3BlobstoreTest {

	@Inject
	private Blobstore blobstore;

//	@Inject
//	private BlobstoreBucket blobstoreBucket;

	@Inject
	private BlobResourceInfoFactory blobResourceInfoFactory;

	private static final String DEFAULT_BUCKET_NAME = "test-clobaframe-blobstore-bucket";

	@Value("${test.blobstore.bucketName}")
	private String bucketName = DEFAULT_BUCKET_NAME;

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testExistBucket() throws InterruptedException {
		//
	}

	@Test
	public void testCreateBucket() throws InterruptedException, IOException {
		// the bucket delete function may fail because the delay, so skip these test.
		if (blobstore.existBucket(bucketName)){
			return;
		}

		// test make
		blobstore.createBucket(bucketName);
		Thread.sleep(5000);

		// test exist
		assertTrue(blobstore.existBucket(bucketName));
	}

	public void testDeleteBucket() {
		// the bucket delete function may fail because the delay, so skip these test.
	}

	@Test
	public void testPut() throws IOException, InterruptedException {

		String key1 = "b001";
		String key2 = "b002";
		String key3 = "b003";

		// check bucket first
		if (!blobstore.existBucket(bucketName)){
			blobstore.createBucket(bucketName);
			Thread.sleep(1000);
		}

		// clean first
		cleanByKey(key1);
		cleanByKey(key2);
		cleanByKey(key3);

		BlobKey blobKey1 = new BlobKey(bucketName, key1);

		// test put blob
		// put blob with meta data.
		Map<String, String> metadata = new HashMap<String, String>();
		metadata.put("author", "test");
		metadata.put("price", "99.0");
		writeContent(blobstore, blobKey1, "hello", metadata);

		// test get blob by key
		BlobResourceInfo blobByKey1 = blobstore.get(blobKey1);
		assertEquals(blobKey1, blobByKey1.getBlobKey());
		assertEquals("text/plain", blobByKey1.getContentType());
		assertEquals(5, blobByKey1.getContentLength());
		assertNotNull(blobByKey1.getLastModified());

		assertEquals(blobByKey1.getMetadata().get("author"), "test");
		assertEquals(blobByKey1.getMetadata().get("price"), "99.0");

		// test get blob content
		assertEquals("hello", readContent(blobByKey1));

		// test get blob content partial
		assertEquals("ll", readContent(blobByKey1,2,2));

		// test overwrite blob content
		writeContent(blobstore, blobKey1, "woo");
		BlobResourceInfo overwriteBlobByKey1 = blobstore.get(blobKey1);
		assertEquals("woo", readContent(overwriteBlobByKey1));

		// test get none-exists blob
		BlobKey blobKeyNoneExists = new BlobKey(bucketName, "noneExists");
		try{
			blobstore.get(blobKeyNoneExists);
			fail();
		}catch(IOException e){
			// pass
		}

		// test delete
		blobstore.delete(blobKey1);
		try{
			blobstore.get(blobKey1);
			fail();
		}catch(IOException e){
			// pass
		}

		// test delete none-exist key
		blobstore.delete(new BlobKey(bucketName, "noneExists"));

		// put public read and reduced redundancy blob, currently can not verify automatically.
		ByteArrayInputStream in1 = new ByteArrayInputStream("foo".getBytes());
		byte[] data1 = "bar".getBytes();

		BlobKey blobKey2 = new BlobKey(bucketName, key2);
		BlobResourceInfo blobByFactory1 = blobResourceInfoFactory.make(blobKey2, "text/plain", in1, 3);

		BlobKey blobKey3 = new BlobKey(bucketName, key3);
		BlobResourceInfo blobByFactory2 = blobResourceInfoFactory.make(blobKey3, "text/plain", data1);

		blobstore.put(blobByFactory1, true, Blobstore.DEFAULT_STORE_PRIOTITY);
		blobstore.put(blobByFactory2, false, Blobstore.MIN_STORE_PRIORITY);

		in1.close();

		BlobResourceInfo blobByKey2 = blobstore.get(blobKey2);
		BlobResourceInfo blobByKey3 = blobstore.get(blobKey3);

		assertEquals("foo", readContent(blobByKey2));
		assertEquals("bar", readContent(blobByKey3));

		// clean up
		cleanByKey(key2);
		cleanByKey(key3);
	}

	public void testPutWithPublic() {
		//
	}

	public void testGet() {
		//
	}

	public void testDelete() {
		//
	}

	@Test
	public void testList() throws IOException, InterruptedException {
		// check bucket first
		if (!blobstore.existBucket(bucketName)){
			blobstore.createBucket(bucketName);
			Thread.sleep(1000);
		}

		BlobKey prefixAll = new BlobKey(bucketName, null);

		// delete exist blobs
		BlobResourceInfoPartialCollection blobs = blobstore.list(prefixAll);
		if (blobs.size() > 0){
			for(BlobResourceInfo blob : blobs){
				blobstore.delete(blob.getBlobKey());
			}
		}

		// test put blob
		writeContent(blobstore, new BlobKey(bucketName, "a"), "a");
		writeContent(blobstore, new BlobKey(bucketName, "r-j001"), "j001");
		writeContent(blobstore, new BlobKey(bucketName, "r-j002"), "j002");
		writeContent(blobstore, new BlobKey(bucketName, "r-c-c001"), "c001");
		writeContent(blobstore, new BlobKey(bucketName, "r"), "r");

		// test list
		BlobResourceInfoPartialCollection blobsByNoPrefix1 = blobstore.list(prefixAll);
		assertEquals(5, blobsByNoPrefix1.size());
		assertFalse(blobsByNoPrefix1.hasMore());

		assertContainsKey(blobsByNoPrefix1, "a");
		assertContainsKey(blobsByNoPrefix1, "r-j001");
		assertContainsKey(blobsByNoPrefix1, "r-j002");
		assertContainsKey(blobsByNoPrefix1, "r-c-c001");
		assertContainsKey(blobsByNoPrefix1, "r");

		BlobResourceInfoPartialCollection blobsByPrefix1 = blobstore.list(new BlobKey(bucketName, "r-"));
		assertEquals(3, blobsByPrefix1.size());
		assertContainsKey(blobsByPrefix1, "r-c-c001");
		assertContainsKey(blobsByPrefix1, "r-j001");
		assertContainsKey(blobsByPrefix1, "r-j002");

		BlobResourceInfoPartialCollection blobsByPrefix2 = blobstore.list(new BlobKey(bucketName, "r-c-"));
		assertEquals(1, blobsByPrefix2.size());
		assertContainsKey(blobsByPrefix2, "r-c-c001");

		// delete all blobs
		BlobResourceInfoPartialCollection blobsByNoPrefix2 = blobstore.list(prefixAll);
		for(BlobResourceInfo blob : blobsByNoPrefix2){
			blobstore.delete(blob.getBlobKey());
		}

		Thread.sleep(5000);
		
		BlobResourceInfoPartialCollection blobsByRemove1 = blobstore.list(prefixAll);
		assertEquals(0, blobsByRemove1.size());
	}

	public void testListNext() {
		//
	}

	private void writeContent(
			Blobstore blobstoreService,
			BlobKey blobKey,
			String content) throws IOException{
		writeContent(blobstoreService, blobKey, content, null);
	}

	private void writeContent(
			Blobstore blobstore,
			BlobKey blobKey,
			String content,
			Map<String, String> metadata) throws IOException{
		byte[] data = content.getBytes();
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		BlobResourceInfo blobResourceInfo = blobResourceInfoFactory.make(blobKey, "text/plain", in, data.length);

		if (metadata != null){
			for(String key : metadata.keySet()){
				blobResourceInfo.addMetadata(key, metadata.get(key));
			}
		}
		blobstore.put(blobResourceInfo);
		in.close();
	}

	private String readContent(
			BlobResourceInfo blob) throws IOException{
		//ResourceContent blobContent = blob.getContentSnapshot();
		InputStream in = blob.getInputStream();
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(in));
		String content = reader.readLine();
		reader.close();
		in.close();
		return content;
	}

	private String readContent(
			BlobResourceInfo blob, long start, long length) throws IOException{
		//ResourceContent blobContent = blob.getContentSnapshot(start, length);
		InputStream in = blob.getInputStream(start, length);
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(in));
		String content = reader.readLine();
		reader.close();
		in.close();
		return content;
	}

	private void cleanByKey(String key){
		BlobKey blobKey = new BlobKey(bucketName, key);
		try{
			//BlobInfo blob1 = blobstore.get(blobKey);
			blobstore.delete(blobKey);
		}catch(IOException e){
			// ignore
		}
	}

	private void assertContainsKey(BlobResourceInfoPartialCollection collection, String key) {
		boolean found = false;
		for (BlobResourceInfo info : collection) {
			if (info.getBlobKey().getKey().equals(key)){
				found = true;
				break;
			}
		}

		assertTrue(found);
	}
}
