package com.amazonaws.kvstranscribestreaming;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.transcribestreaming.model.Result;
import software.amazon.awssdk.services.transcribestreaming.model.TranscriptEvent;

import java.text.NumberFormat;
import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;


/**
 * TranscribedSegmentWriter writes the transcript segments to DynamoDB
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
public class TranscribedSegmentWriter {

    private String contactId;
    private DynamoDB ddbClient;
    private Boolean consoleLogTranscriptFlag;
    private String participantRole; // "Agent" or "Customer"
    private static final boolean SAVE_PARTIAL_TRANSCRIPTS = Boolean.parseBoolean(System.getenv("SAVE_PARTIAL_TRANSCRIPTS"));
    private static final Logger logger = LoggerFactory.getLogger(TranscribedSegmentWriter.class);


    public TranscribedSegmentWriter(String contactId, DynamoDB ddbClient, Boolean consoleLogTranscriptFlag) {

        this.contactId = Validate.notNull(contactId);
        this.ddbClient = Validate.notNull(ddbClient);
        this.consoleLogTranscriptFlag = Validate.notNull(consoleLogTranscriptFlag);

    }


    public void setParticipantRole(String participantRole) {
        this.participantRole = Validate.notNull(participantRole);
    }

    public String getContactId() {

        return this.contactId;
    }

    public DynamoDB getDdbClient() {

        return this.ddbClient;
    }


    public void writeToDynamoDB(TranscriptEvent transcriptEvent, String tableName, String conversationTableName) {
        logger.info("Table name: " + tableName);
        logger.info("Transcription event: " + transcriptEvent.transcript().toString());
        List<Result> results = transcriptEvent.transcript().results();
        if (results.size() > 0) {

            Result result = results.get(0);

            if (SAVE_PARTIAL_TRANSCRIPTS || !result.isPartial()) {
                try {
                    Item ddbItem = toDynamoDbItem(result);
                    if (ddbItem != null) {
                        // Write the individual transcript to the transcript table
                        getDdbClient().getTable(tableName).putItem(ddbItem);

                        // Update the conversation table with the SegmentId
                        updateConversationTable(result.resultId(), conversationTableName);
                    }

                } catch (Exception e) {
                    logger.error("Exception while writing to DynamoDB: ", e);
                }
            }
        }
    }


    private Item toDynamoDbItem(Result result) {

        String contactId = this.getContactId();
        Item ddbItem = null;

        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumFractionDigits(3);
        nf.setMaximumFractionDigits(3);

        if (result.alternatives().size() > 0) {
            if (!result.alternatives().get(0).transcript().isEmpty()) {

                Instant now = Instant.now();
                ddbItem = new Item()
                        .withKeyComponent("ContactId", contactId)
                        .withKeyComponent("StartTime", result.startTime())
                        .withString("SegmentId", result.resultId())
                        .withDouble("EndTime", result.endTime())
                        .withString("Transcript", result.alternatives().get(0).transcript())
                        .withBoolean("IsPartial", result.isPartial())
                        // LoggedOn is an ISO-8601 string representation of when the entry was created
                        .withString("LoggedOn", now.toString())
                        // expire after a week by default
                        .withDouble("ExpiresAfter", now.plusSeconds(7 * 24 * 3600).toEpochMilli() / 1000);

                if (consoleLogTranscriptFlag) {
                    logger.info(String.format("Thread %s %d: [%s, %s] - %s",
                            Thread.currentThread().getName(),
                            System.currentTimeMillis(),
                            nf.format(result.startTime()),
                            nf.format(result.endTime()),
                            result.alternatives().get(0).transcript()));
                }
            }
        }

        return ddbItem;
    }

    private void updateConversationTable(String segmentId, String conversationTableName) {
        if (participantRole == null) {
            logger.error("Participant role is not set. Cannot update conversation table.");
            return;
        }

        Table conversationTable = getDdbClient().getTable(conversationTableName);
        String contactId = this.getContactId();

        try {
            String idsAttributeName;
            // Determine which list to update based on participantRole
            if ("Agent".equalsIgnoreCase(this.participantRole)) {
                idsAttributeName = "AgentConversationMessageIds";
            } else if ("Customer".equalsIgnoreCase(this.participantRole)) {
                idsAttributeName = "UserConversationMessageIds";
            } else {
                throw new IllegalArgumentException("Invalid participantRole: " + this.participantRole);
            }

            // Build the update expression to append the SegmentId to the list
            UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                    .withPrimaryKey("ContactId", contactId)
                    .withUpdateExpression("SET #ids = list_append(if_not_exists(#ids, :empty_list), :segmentId), LLMStatus = if_not_exists(LLMStatus, :empty_string)")
                    .withNameMap(new NameMap().with("#ids", idsAttributeName))
                    .withValueMap(new ValueMap()
                            .withList(":segmentId", Collections.singletonList(segmentId))
                            .withList(":empty_list", new ArrayList<>())
                            .withString(":empty_string", ""))
                    .withReturnValues(ReturnValue.UPDATED_NEW);

            // Update the conversation table in DynamoDB
            conversationTable.updateItem(updateItemSpec);

        } catch (Exception e) {
            logger.error("Failed to update conversation table: ", e);
        }
    }
}
