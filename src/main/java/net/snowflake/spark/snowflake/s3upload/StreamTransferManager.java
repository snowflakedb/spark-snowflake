package net.snowflake.spark.snowflake.s3upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

// @formatter:off
/**
 * Manages streaming of data to S3 without knowing the size beforehand and without keeping it all in memory or
 * writing to disk.
 * <p>
 * The data is split into chunks and uploaded using the multipart upload API.
 * The uploading is done on separate threads, the number of which is configured by the user.
 * <p>
 * After creating an instance with details of the upload, use {@link StreamTransferManager#getMultiPartOutputStreams()}
 * to get a list
 * of {@link MultiPartOutputStream}s. As you write data to these streams, call
 * {@link MultiPartOutputStream#checkSize()} regularly. When you finish, call {@link MultiPartOutputStream#close()}.
 * Parts will be uploaded to S3 as you write.
 * <p>
 * Once all streams have been closed, call {@link StreamTransferManager#complete()}. Alternatively you can call
 * {@link StreamTransferManager#abort()}
 * at any point if needed.
 * <p>
 * Here is an example. A lot of the code relates to setting up threads for creating data unrelated to the library. The
 * essential parts are commented.
* <pre>{@code
    AmazonS3Client client = new AmazonS3Client(awsCreds);
    int numStreams = 2;
    int numUploadThreads = 2;
    int queueCapacity = 2;
    int partSize = 5;

    // Setting up
    final StreamTransferManager manager = new StreamTransferManager(bucket, key, client, numStreams,
                                                                    numUploadThreads, queueCapacity, partSize);
    final List<MultiPartOutputStream> streams = manager.getMultiPartOutputStreams();

    ExecutorService pool = Executors.newFixedThreadPool(numStreams);
    for (int i = 0; i < numStreams; i++) {
        final int streamIndex = i;
        pool.submit(new Runnable() {
            public void run() {
                try {
                    MultiPartOutputStream outputStream = streams.get(streamIndex);
                    for (int lineNum = 0; lineNum < 1000000; lineNum++) {
                        String line = generateData(streamIndex, lineNum);

                        // Writing data and potentially sending off a part
                        outputStream.write(line.getBytes());
                        try {
                            outputStream.checkSize();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    // The stream must be closed once all the data has been written
                    outputStream.close();
                } catch (Exception e) {

                    // Aborts all uploads
                    manager.abort(e);
                }
            }
        });
    }
    pool.shutdown();
    pool.awaitTermination(5, TimeUnit.SECONDS);

    // Finishing off
    manager.complete();
 * }</pre>
 * <p>
 * The final file on S3 will then usually be the result of concatenating all the data written to each stream,
 * in the order that the streams were in in the list obtained from {@code getMultiPartOutputStreams()}. However this
 * may not be true if multiple streams are used and some of them produce less than 5 MB of data. This is because the multipart
 * upload API does not allow the uploading of more than one part smaller than 5 MB, which leads to fundamental limits
 * on what this class can accomplish. If order of data is important to you, then either use only one stream or ensure
 * that you write at least 5 MB to every stream.
 * <p>
 * While performing the multipart upload this class will create instances of {@link InitiateMultipartUploadRequest},
 * {@link UploadPartRequest}, and {@link CompleteMultipartUploadRequest}, fill in the essential details, and send them
 * off. If you need to add additional details then override the appropriate {@code customise*Request} methods and
 * set the required properties within.
 * <p>
 * This class does not perform retries when uploading. If an exception is thrown at any stage the upload will be aborted and the
 * exception rethrown, wrapped in a {@code RuntimeException}.
 *
 * @author Alex Hall
 */
// @formatter:on
public class StreamTransferManager {

    private static final Logger log = LoggerFactory.getLogger(StreamTransferManager.class);

    protected final String bucketName;
    protected final String putKey;
    protected final AmazonS3 s3Client;
    protected final String uploadId;
    private final List<PartETag> partETags;
    private final List<MultiPartOutputStream> multiPartOutputStreams;
    private final ExecutorServiceResultsHandler<Void> executorServiceResultsHandler;
    private final BlockingQueue<StreamPart> queue;
    private int finishedCount = 0;
    private StreamPart leftoverStreamPart = null;
    private final Object leftoverStreamPartLock = new Object();
    private boolean isAborting = false;
    private static final int MAX_PART_NUMBER = 10000;

    /**
     * Initiates a multipart upload to S3 using the first three parameters. Creates several
     * {@link MultiPartOutputStream}s and threads to upload the parts they produce in parallel.
     * Parts that have been produced sit in a queue of specified capacity while they wait for a thread to upload them.
     * The worst case memory usage is therefore {@code (numStreams + numUploadThreads + queueCapacity) * partSize},
     * while higher values for these first three parameters may lead to better resource usage and throughput.
     * <p>
     * S3 allows at most 10 000 parts to be uploaded. This means that if you are uploading very large files, the part
     * size must be big enough to compensate. Moreover the part numbers are distributed equally among streams so keep
     * this in mind if you might write much more data to some streams than others.
     *
     * @param numStreams       the number of multiPartOutputStreams that will be created for you to write to.
     * @param numUploadThreads the number of threads that will upload parts as they are produced.
     * @param queueCapacity    the capacity of the queue that holds parts yet to be uploaded.
     * @param partSize         the minimum size of each part in MB before it gets uploaded. Minimum is 5 due to limitations of S3.
     *                         More than 500 is not useful in most cases as this corresponds to the limit of 5 TB total for any upload.
     */
    public StreamTransferManager(String bucketName,
                                 String putKey,
                                 AmazonS3 s3Client,
                                 ObjectMetadata meta,
                                 int numStreams,
                                 int numUploadThreads,
                                 int queueCapacity,
                                 int partSize) {
        if (numStreams <= 0) {
            throw new IllegalArgumentException("There must be at least one stream");
        }
        if (numUploadThreads <= 0) {
            throw new IllegalArgumentException("There must be at least one upload thread");
        }
        partSize *= 1024 * 1024;
        if (partSize < MultiPartOutputStream.S3_MIN_PART_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "The given part size (%d) is less than 5 MB.", partSize));
        }

        this.bucketName = bucketName;
        this.putKey = putKey;
        this.s3Client = s3Client;
        queue = new ArrayBlockingQueue<StreamPart>(queueCapacity);

        //log.debug("Initiating multipart upload to {}/{}", bucketName, putKey);
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, putKey);
        initRequest.setObjectMetadata(meta);
        customiseInitiateRequest(initRequest);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        uploadId = initResponse.getUploadId();
        //log.info("Initiated multipart upload to {}/{} with full ID {}", bucketName, putKey, uploadId);
        try {
            partETags = new ArrayList<PartETag>();
            multiPartOutputStreams = new ArrayList<MultiPartOutputStream>();
            ExecutorService threadPool = Executors.newFixedThreadPool(numStreams);

            int partNumberStart = 1;

            for (int i = 0; i < numStreams; i++) {
                int partNumberEnd = (i + 1) * MAX_PART_NUMBER / numStreams + 1;
                MultiPartOutputStream multiPartOutputStream = new MultiPartOutputStream(partNumberStart, partNumberEnd, partSize, queue);
                partNumberStart = partNumberEnd;
                multiPartOutputStreams.add(multiPartOutputStream);
            }

            executorServiceResultsHandler = new ExecutorServiceResultsHandler<Void>(threadPool);
            for (int i = 0; i < numUploadThreads; i++) {
                executorServiceResultsHandler.submit(new UploadTask());
            }
            executorServiceResultsHandler.finishedSubmitting();
        } catch (Throwable e) {
            abort(e);
            throw new RuntimeException("Unexpected error occurred while setting up streams and threads for upload: this likely indicates a bug in this class.", e);
        }

    }

    public List<MultiPartOutputStream> getMultiPartOutputStreams() {
        return multiPartOutputStreams;
    }

    /**
     * Blocks while waiting for the threads uploading the contents of the streams returned
     * by {@link StreamTransferManager#getMultiPartOutputStreams()} to finish, then sends a request to S3 to complete
     * the upload. For the former to complete, it's essential that every stream is closed, otherwise the upload
     * threads will block forever waiting for more data.
     */
    public void complete() {
        try {
            //log.debug("{}: Waiting for pool termination", this);
            executorServiceResultsHandler.awaitCompletion();
            //log.debug("{}: Pool terminated", this);
            if (leftoverStreamPart != null) {
                //log.info("{}: Uploading leftover stream {}", leftoverStreamPart);
                uploadStreamPart(leftoverStreamPart);
                //log.debug("{}: Leftover uploaded", this);
            }
            //log.debug("{}: Completing", this);
            CompleteMultipartUploadRequest completeRequest = new
                    CompleteMultipartUploadRequest(
                    bucketName,
                    putKey,
                    uploadId,
                    partETags);
            customiseCompleteRequest(completeRequest);
            s3Client.completeMultipartUpload(completeRequest);
            //log.info("{}: Completed", this);
        } catch (Throwable e) {
            abort(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Aborts the upload and logs a message including the stack trace of the given throwable.
     */
    public void abort(Throwable throwable) {
        //log.error("{}: Abort called due to error:", this, throwable);
        abort();
    }

    /**
     * Aborts the upload. Repeated calls have no effect.
     */
    public void abort() {
        synchronized (this) {
            if (isAborting) {
                return;
            }
            isAborting = true;
        }
        executorServiceResultsHandler.abort();
        //log.debug("{}: Aborting", this);
        AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest(
                bucketName, putKey, uploadId);
        s3Client.abortMultipartUpload(abortMultipartUploadRequest);
        //log.info("{}: Aborted", this);
    }

    private class UploadTask implements Callable<Void> {

        @Override
        public Void call() {
            try {
                while (true) {
                    StreamPart part;
                    synchronized (queue) {
                        if (finishedCount < multiPartOutputStreams.size()) {
                            part = queue.take();
                            if (part == StreamPart.POISON) {
                                finishedCount++;
                                continue;
                            }
                        } else {
                            break;
                        }
                    }
                    if (part.size() < MultiPartOutputStream.S3_MIN_PART_SIZE) {
                    /*
                    Each stream does its best to avoid producing parts smaller than 5 MB, but if a user doesn't
                    write that much data there's nothing that can be done. These are considered 'leftover' parts,
                    and must be merged with other leftovers to try producing a part bigger than 5 MB which can be
                    uploaded without problems. After the threads have completed there may be at most one leftover
                    part remaining, which S3 can accept. It is uploaded in the complete() method.
                    */
                        //log.debug("{}: Received part {} < 5 MB that needs to be handled as 'leftover'", this, part);
                        StreamPart originalPart = part;
                        part = null;
                        synchronized (leftoverStreamPartLock) {
                            if (leftoverStreamPart == null) {
                                leftoverStreamPart = originalPart;
                                //log.debug("{}: Created new leftover part {}", this, leftoverStreamPart);
                            } else {
                                /*
                                Try to preserve order within the data by appending the part with the higher number
                                to the part with the lower number. This is not meant to produce a perfect solution:
                                if the client is producing multiple leftover parts all bets are off on order.
                                */
                                if (leftoverStreamPart.getPartNumber() > originalPart.getPartNumber()) {
                                    StreamPart temp = originalPart;
                                    originalPart = leftoverStreamPart;
                                    leftoverStreamPart = temp;
                                }
                                leftoverStreamPart.getOutputStream().append(originalPart.getOutputStream());
                                //log.debug("{}: Merged with existing leftover part to create {}", this, leftoverStreamPart);
                                if (leftoverStreamPart.size() >= MultiPartOutputStream.S3_MIN_PART_SIZE) {
                                    //log.debug("{}: Leftover part can now be uploaded as normal and reset", this);
                                    part = leftoverStreamPart;
                                    leftoverStreamPart = null;
                                }
                            }
                        }
                    }
                    if (part != null) {
                        uploadStreamPart(part);
                    }
                }
            } catch (Throwable t) {
                abort(t);
                throw new RuntimeException(t);
            }

            return null;
        }

    }

    private void uploadStreamPart(StreamPart part) {
        //log.debug("{}: Uploading {}", this, part);

        UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(bucketName).withKey(putKey)
                .withUploadId(uploadId).withPartNumber(part.getPartNumber())
                .withInputStream(part.getInputStream())
                .withPartSize(part.size());
        customiseUploadPartRequest(uploadRequest);

        UploadPartResult uploadPartResult = s3Client.uploadPart(uploadRequest);
        PartETag partETag = uploadPartResult.getPartETag();
        partETags.add(partETag);
        //log.info("{}: Finished uploading {}", this, part);
    }

    @Override
    public String toString() {
        return String.format("[Manager uploading to %s/%s with id %s]",
                bucketName, putKey, Utils.skipMiddle(uploadId, 21));
    }

    // These methods are intended to be overridden for more specific interactions with the AWS API.

    public void customiseInitiateRequest(InitiateMultipartUploadRequest request) {
    }

    public void customiseUploadPartRequest(UploadPartRequest request) {
    }

    public void customiseCompleteRequest(CompleteMultipartUploadRequest request) {
    }

}