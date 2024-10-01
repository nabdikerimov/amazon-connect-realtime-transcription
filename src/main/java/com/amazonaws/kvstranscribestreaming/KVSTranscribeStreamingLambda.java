package com.amazonaws.kvstranscribestreaming;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.kinesisvideo.parser.ebml.InputStreamParserByteSource;
import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadataVisitor;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.transcribestreaming.FileByteToAudioEventSubscription;
import com.amazonaws.transcribestreaming.KVSByteToAudioEventSubscription;
import com.amazonaws.transcribestreaming.StreamTranscriptionBehaviorImpl;
import com.amazonaws.transcribestreaming.TranscribeStreamingRetryClient;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.transcribestreaming.model.AudioStream;
import software.amazon.awssdk.services.transcribestreaming.model.LanguageCode;
import software.amazon.awssdk.services.transcribestreaming.model.MediaEncoding;
import software.amazon.awssdk.services.transcribestreaming.model.StartStreamTranscriptionRequest;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Optional;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Demonstrate Amazon Connect's real-time transcription feature using AWS Kinesis Video Streams and AWS Transcribe.
 * The data flow is :
 * <p>
 * Amazon Connect => AWS KVS => AWS Transcribe => AWS DynamoDB
 *
 * <p>Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.</p>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
public class KVSTranscribeStreamingLambda implements RequestHandler<TranscriptionRequest, String> {

    private static final Regions REGION = Regions.fromName(System.getenv("APP_REGION"));
    private static final Regions TRANSCRIBE_REGION = Regions.fromName(System.getenv("TRANSCRIBE_REGION"));
    private static final String TRANSCRIBE_ENDPOINT = "https://transcribestreaming." + TRANSCRIBE_REGION.getName() + ".amazonaws.com";
    private static final String RECORDINGS_BUCKET_NAME = System.getenv("RECORDINGS_BUCKET_NAME");
    private static final String RECORDINGS_KEY_PREFIX = System.getenv("RECORDINGS_KEY_PREFIX");
    private static final String INPUT_KEY_PREFIX = System.getenv("INPUT_KEY_PREFIX");
    private static final boolean CONSOLE_LOG_TRANSCRIPT_FLAG = Boolean.parseBoolean(System.getenv("CONSOLE_LOG_TRANSCRIPT_FLAG"));
    private static final boolean RECORDINGS_PUBLIC_READ_ACL = Boolean.parseBoolean(System.getenv("RECORDINGS_PUBLIC_READ_ACL"));
    private static final String START_SELECTOR_TYPE = System.getenv("START_SELECTOR_TYPE");
    private static final String TABLE_CALLER_TRANSCRIPT = System.getenv("TABLE_CALLER_TRANSCRIPT");
    private static final String TABLE_CALLER_TRANSCRIPT_TO_CUSTOMER = System.getenv("TABLE_CALLER_TRANSCRIPT_TO_CUSTOMER");
    private static final String CONVERSATION_TABLE_NAME = System.getenv("CONVERSATION_TABLE_NAME");

    private static final Logger logger = LoggerFactory.getLogger(KVSTranscribeStreamingLambda.class);
    public static final MetricsUtil metricsUtil = new MetricsUtil(AmazonCloudWatchClientBuilder.defaultClient());
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    // SegmentWriter saves Transcription segments to DynamoDB
    private TranscribedSegmentWriter fromCustomerSegmentWriter = null;
    private TranscribedSegmentWriter toCustomerSegmentWriter = null;


    @Override
    public String handleRequest(TranscriptionRequest request, Context context) {

        logger.info("received request : " + request.toString());
        logger.info("received context: " + context.toString());

        try {
            // validate the request
            request.validate();

            // create a SegmentWriter to be able to save off transcription results
            AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
            builder.setRegion(REGION.getName());
            DynamoDB dynamoDB = new DynamoDB(builder.build());

            fromCustomerSegmentWriter = new TranscribedSegmentWriter(request.getConnectContactId(), dynamoDB,
                    CONSOLE_LOG_TRANSCRIPT_FLAG);
            fromCustomerSegmentWriter.setParticipantRole("Customer");

            toCustomerSegmentWriter = new TranscribedSegmentWriter(request.getConnectContactId(), dynamoDB,
                    CONSOLE_LOG_TRANSCRIPT_FLAG);
            toCustomerSegmentWriter.setParticipantRole("Agent");

            // If an inputFileName has been provided in the request, stream audio from the file to Transcribe
            if (request.getInputFileName() != null) {
                startFileToTranscribeStreaming(request.getInputFileName(), request.getLanguageCode());
            }
            // Else start streaming between KVS and Transcribe
            else {
                startKVSToTranscribeStreaming(request.getStreamARN(), request.getStartFragmentNum(), request.getConnectContactId(),
                        request.isTranscriptionEnabled(), request.getLanguageCode(), request.getSaveCallRecording(),
                        request.isStreamAudioFromCustomer(), request.isStreamAudioToCustomer());
            }

            return "{ \"result\": \"Success\" }";

        } catch (Exception e) {
            logger.error("KVS to Transcribe Streaming failed with: ", e);
            return "{ \"result\": \"Failed\" }";
        }
    }


    private void startKVSToTranscribeStreaming(String streamARN, String startFragmentNum, String contactId, boolean transcribeEnabled,
                                               Optional<String> languageCode, Optional<Boolean> saveCallRecording,
                                               boolean isStreamAudioFromCustomerEnabled, boolean isStreamAudioToCustomerEnabled) throws Exception {
        String streamName = streamARN.substring(streamARN.indexOf("/") + 1, streamARN.lastIndexOf("/"));

        KVSStreamTrackObject kvsStreamTrackObjectFromCustomer = null;
        KVSStreamTrackObject kvsStreamTrackObjectToCustomer = null;

        if (isStreamAudioFromCustomerEnabled) {
            kvsStreamTrackObjectFromCustomer = getKVSStreamTrackObject(streamName, startFragmentNum, KVSUtils.TrackName.AUDIO_FROM_CUSTOMER.getName(), contactId);
        }
        if (isStreamAudioToCustomerEnabled) {
            kvsStreamTrackObjectToCustomer = getKVSStreamTrackObject(streamName, startFragmentNum, KVSUtils.TrackName.AUDIO_TO_CUSTOMER.getName(), contactId);
        }

        if (transcribeEnabled) {
            try (TranscribeStreamingRetryClient client = new TranscribeStreamingRetryClient(getTranscribeCredentials(),
                    TRANSCRIBE_ENDPOINT, TRANSCRIBE_REGION, metricsUtil)) {

                logger.info("Calling Transcribe service...");
                CompletableFuture<Void> fromCustomerResult = null;
                CompletableFuture<Void> toCustomerResult = null;

                if (kvsStreamTrackObjectFromCustomer != null) {
                    fromCustomerResult = getStartStreamingTranscriptionFuture(kvsStreamTrackObjectFromCustomer,
                            languageCode, contactId, client, fromCustomerSegmentWriter, TABLE_CALLER_TRANSCRIPT, KVSUtils.TrackName.AUDIO_FROM_CUSTOMER.getName());
                }

                if (kvsStreamTrackObjectToCustomer != null) {
                    toCustomerResult = getStartStreamingTranscriptionFuture(kvsStreamTrackObjectToCustomer,
                            languageCode, contactId, client, toCustomerSegmentWriter, TABLE_CALLER_TRANSCRIPT_TO_CUSTOMER, KVSUtils.TrackName.AUDIO_TO_CUSTOMER.getName());
                }

                // Synchronous wait for stream to close, and close client connection
                // Timeout of 890 seconds because the Lambda function can be run for at most 15 mins (~890 secs)
                if (fromCustomerResult != null) {
                    fromCustomerResult.get(890, TimeUnit.SECONDS);
                }

                if (toCustomerResult != null) {
                    toCustomerResult.get(890, TimeUnit.SECONDS);
                }

            } catch (TimeoutException e) {
                logger.debug("Timing out KVS to Transcribe Streaming after 890 sec");

            } catch (Exception e) {
                logger.error("Error during streaming: ", e);
                throw e;

            } finally {
                if (kvsStreamTrackObjectFromCustomer != null) {
                    closeFileAndUploadRawAudio(kvsStreamTrackObjectFromCustomer, contactId, saveCallRecording);
                }
                if (kvsStreamTrackObjectToCustomer != null) {
                    closeFileAndUploadRawAudio(kvsStreamTrackObjectToCustomer, contactId, saveCallRecording);
                }
            }
        } else {
            try {
                logger.info("Saving audio bytes to location");

                // Write audio bytes from the KVS stream to the temporary file
                if (kvsStreamTrackObjectFromCustomer != null) {
                    writeAudioBytesToKvsStream(kvsStreamTrackObjectFromCustomer, contactId);
                }
                if (kvsStreamTrackObjectToCustomer != null) {
                    writeAudioBytesToKvsStream(kvsStreamTrackObjectToCustomer, contactId);
                }

            } finally {
                if (kvsStreamTrackObjectFromCustomer != null) {
                    closeFileAndUploadRawAudio(kvsStreamTrackObjectFromCustomer, contactId, saveCallRecording);
                }
                if (kvsStreamTrackObjectToCustomer != null) {
                    closeFileAndUploadRawAudio(kvsStreamTrackObjectToCustomer, contactId, saveCallRecording);
                }
            }
        }
    }


    private void startFileToTranscribeStreaming(String inputFileName, Optional<String> languageCode) throws Exception {

        // get the audio from S3
        String audioFilePath = "/tmp/" + inputFileName;
        AudioUtils.fetchAudio(REGION, RECORDINGS_BUCKET_NAME, INPUT_KEY_PREFIX + inputFileName, audioFilePath, getAWSCredentials());

        // Now get the stream to be transcribed (and written out as transcript segments)
        InputStream inputStream = new FileInputStream(audioFilePath);

        try (TranscribeStreamingRetryClient client = new TranscribeStreamingRetryClient(getTranscribeCredentials(),
                TRANSCRIBE_ENDPOINT, TRANSCRIBE_REGION, metricsUtil)) {

            logger.info("Calling Transcribe service...");

            CompletableFuture<Void> result = client.startStreamTranscription(
                    // since we're definitely working with telephony audio, we know that's 8 kHz
                    getRequest(8000, languageCode),
                    new FileAudioStreamPublisher(inputStream),
                    new StreamTranscriptionBehaviorImpl(fromCustomerSegmentWriter, TABLE_CALLER_TRANSCRIPT, CONVERSATION_TABLE_NAME),
                    "None"
            );

            // Synchronous wait for stream to close, and close client connection
            // Timeout of 890 seconds because the Lambda function can be run for at most 15 mins (~890 secs)
            result.get(890, TimeUnit.SECONDS);

        } catch (TimeoutException e) {
            logger.debug("Timing out Audio file to Transcribe Streaming after 890 sec");

        } catch (Exception e) {
            logger.error("Error during streaming: ", e);
            throw e;

        }
    }


    private void closeFileAndUploadRawAudio(KVSStreamTrackObject kvsStreamTrackObject, String contactId, Optional<Boolean> saveCallRecording) throws IOException {

        kvsStreamTrackObject.getInputStream().close();
        kvsStreamTrackObject.getOutputStream().close();

        // Upload the Raw Audio file to S3
        if ((saveCallRecording.isPresent() ? saveCallRecording.get() : false) && (new File(kvsStreamTrackObject.getSaveAudioFilePath().toString()).length() > 0)) {
            AudioUtils.uploadRawAudio(REGION, RECORDINGS_BUCKET_NAME, RECORDINGS_KEY_PREFIX, kvsStreamTrackObject.getSaveAudioFilePath().toString(), contactId, RECORDINGS_PUBLIC_READ_ACL,
                    getAWSCredentials());
        } else {
            logger.info("Skipping upload to S3. saveCallRecording was disabled or audio file has 0 bytes: " + kvsStreamTrackObject.getSaveAudioFilePath().toString());
        }
        // Delete raw files from the lambda function
        File fileToDelete = new File(kvsStreamTrackObject.getSaveAudioFilePath().toString());
        if (fileToDelete != null) {
            Boolean deletionResult = fileToDelete.delete();
            logger.info("Raw file for contactId {} deletion result: {}", contactId, deletionResult);
        }
    }


    private KVSStreamTrackObject getKVSStreamTrackObject(String streamName, String startFragmentNum, String trackName,
                                                         String contactId) throws FileNotFoundException {
        InputStream kvsInputStream = KVSUtils.getInputStreamFromKVS(streamName, REGION, startFragmentNum, getAWSCredentials(), START_SELECTOR_TYPE);
        StreamingMkvReader streamingMkvReader = StreamingMkvReader.createDefault(new InputStreamParserByteSource(kvsInputStream));

        KVSContactTagProcessor tagProcessor = new KVSContactTagProcessor(contactId);
        FragmentMetadataVisitor fragmentVisitor = FragmentMetadataVisitor.create(Optional.of(tagProcessor));

        String fileName = String.format("%s_%s_%s.raw", contactId, DATE_FORMAT.format(new Date()), trackName);
        Path saveAudioFilePath = Paths.get("/tmp", fileName);
        FileOutputStream fileOutputStream = new FileOutputStream(saveAudioFilePath.toString());

        return new KVSStreamTrackObject(kvsInputStream, streamingMkvReader, tagProcessor, fragmentVisitor, saveAudioFilePath, fileOutputStream, trackName);
    }

    private CompletableFuture<Void> getStartStreamingTranscriptionFuture(KVSStreamTrackObject kvsStreamTrackObject, Optional<String> languageCode,
                                                                         String contactId, TranscribeStreamingRetryClient client,
                                                                         TranscribedSegmentWriter transcribedSegmentWriter,
                                                                         String tableName, String channel) {
        return client.startStreamTranscription(
                // since we're definitely working with telephony audio, we know that's 8 kHz
                getRequest(8000, languageCode),
                new KVSAudioStreamPublisher(
                        kvsStreamTrackObject.getStreamingMkvReader(),
                        contactId,
                        kvsStreamTrackObject.getOutputStream(),
                        kvsStreamTrackObject.getTagProcessor(),
                        kvsStreamTrackObject.getFragmentVisitor(),
                        kvsStreamTrackObject.getTrackName()),
                new StreamTranscriptionBehaviorImpl(transcribedSegmentWriter, tableName, CONVERSATION_TABLE_NAME),
                channel
        );
    }

    private void writeAudioBytesToKvsStream(KVSStreamTrackObject kvsStreamTrackObject, String contactId) throws Exception {

        ByteBuffer audioBuffer = KVSUtils.getByteBufferFromStream(kvsStreamTrackObject.getStreamingMkvReader(),
                kvsStreamTrackObject.getFragmentVisitor(), kvsStreamTrackObject.getTagProcessor(), contactId, kvsStreamTrackObject.getTrackName());

        while (audioBuffer.remaining() > 0) {
            byte[] audioBytes = new byte[audioBuffer.remaining()];
            audioBuffer.get(audioBytes);
            kvsStreamTrackObject.getOutputStream().write(audioBytes);
            audioBuffer = KVSUtils.getByteBufferFromStream(kvsStreamTrackObject.getStreamingMkvReader(),
                    kvsStreamTrackObject.getFragmentVisitor(), kvsStreamTrackObject.getTagProcessor(), contactId, kvsStreamTrackObject.getTrackName());
        }
    }


    private static AWSCredentialsProvider getAWSCredentials() {
        return DefaultAWSCredentialsProviderChain.getInstance();
    }


    private static AwsCredentialsProvider getTranscribeCredentials() {
        return DefaultCredentialsProvider.create();
    }

    private static StartStreamTranscriptionRequest getRequest(Integer mediaSampleRateHertz, Optional<String> languageCode) {

        return StartStreamTranscriptionRequest.builder()
                .languageCode(languageCode.orElse(LanguageCode.EN_US.toString()))
                .mediaEncoding(MediaEncoding.PCM)
                .mediaSampleRateHertz(mediaSampleRateHertz)
                .build();
    }

    private static class KVSAudioStreamPublisher implements Publisher<AudioStream> {
        private final StreamingMkvReader streamingMkvReader;
        private final String contactId;
        private final OutputStream outputStream;
        private final KVSContactTagProcessor tagProcessor;
        private final FragmentMetadataVisitor fragmentVisitor;
        private final String track;

        private KVSAudioStreamPublisher(StreamingMkvReader streamingMkvReader, String contactId, OutputStream outputStream,
                                        KVSContactTagProcessor tagProcessor, FragmentMetadataVisitor fragmentVisitor,
                                        String track) {
            this.streamingMkvReader = streamingMkvReader;
            this.contactId = contactId;
            this.outputStream = outputStream;
            this.tagProcessor = tagProcessor;
            this.fragmentVisitor = fragmentVisitor;
            this.track = track;
        }

        @Override
        public void subscribe(Subscriber<? super AudioStream> s) {
            s.onSubscribe(new KVSByteToAudioEventSubscription(s, streamingMkvReader, contactId, outputStream, tagProcessor, fragmentVisitor, track));
        }
    }

    private static class FileAudioStreamPublisher implements Publisher<AudioStream> {

        private final InputStream inputStream;

        private FileAudioStreamPublisher(InputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public void subscribe(Subscriber<? super AudioStream> s) {
            s.onSubscribe(new FileByteToAudioEventSubscription(s, inputStream));
        }
    }
}
