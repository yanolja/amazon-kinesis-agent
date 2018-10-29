/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file.
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.amazon.kinesis.streaming.agent.processing.processors;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IJSONPrinter;
import com.amazon.kinesis.streaming.agent.processing.utils.ProcessingUtilsFactory;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeTagsRequest;
import com.amazonaws.services.ec2.model.DescribeTagsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.TagDescription;
import com.amazonaws.util.EC2MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parse the log entries from log file, and convert the log entries into JSON.
 *
 * Configuration of this converter looks like:
 * {
 *     "optionName": "ADDEC2METADATA"
 * }
 *
 * @author buholzer
 *
 */
public class AddEC2MetadataConverter implements IDataConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AddEC2MetadataConverter.class);
  private IJSONPrinter jsonProducer;
  private Map<String, Object> metadata;
  private long metadataTimestamp;
  private long metadataTTL = 1000 * 60 * 60; // Update metadata every hour

  private final List<String> tagFields;
  public AddEC2MetadataConverter(Configuration config) {
    jsonProducer = ProcessingUtilsFactory.getPrinter(config);

    if (config.containsKey("metadataTTL")) {
      try {
        metadataTTL = config.readInteger("metadataTTL") * 1000;
        LOGGER.info("Setting metadata TTL to " + metadataTTL + " millis");
     } catch(Exception ex) {
        LOGGER.warn("Error converting metadataTTL, ignoring");
     }
    }

    tagFields = config.readList("tagFields", String.class, new ArrayList<String>());

    refreshEC2Metadata();
  }

  @Override
  public ByteBuffer convert(ByteBuffer data) throws DataConversionException {

    if ((metadataTimestamp + metadataTTL) < System.currentTimeMillis()) refreshEC2Metadata();

    if (metadata == null || metadata.isEmpty()) {
      LOGGER.warn("Unable to append metadata, no metadata found");
      return data;
    }

    String dataStr = ByteBuffers.toString(data, StandardCharsets.UTF_8);

    if (dataStr.endsWith(NEW_LINE)) {
      dataStr = dataStr.substring(0, (dataStr.length() - NEW_LINE.length()));
    }

    LinkedHashMap<String,Object> dataObj = new LinkedHashMap<>();

    dataObj.put("data", dataStr);

    // Appending EC2 metadata
    dataObj.putAll(metadata);

    String dataJson = jsonProducer.writeAsString(dataObj) + NEW_LINE;
    return ByteBuffer.wrap(dataJson.getBytes(StandardCharsets.UTF_8));
  }

  private void refreshEC2Metadata() {
    LOGGER.info("Refreshing EC2 metadata");

    metadataTimestamp = System.currentTimeMillis();
    
    try {
      EC2MetadataUtils.InstanceInfo info = EC2MetadataUtils.getInstanceInfo();

      metadata = new LinkedHashMap<String, Object>();
      metadata.put("privateIp", info.getPrivateIp());
      metadata.put("instanceId", info.getInstanceId());

      final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
      DescribeTagsResult result = ec2.describeTags(
              new DescribeTagsRequest().withFilters(
                      new Filter().withName("resource-id").withValues(info.getInstanceId())));
      List<TagDescription> tags = result.getTags();

      Map<String, Object> metadataTags = new LinkedHashMap<>();

      for (TagDescription tag : tags) {
        String key = tag.getKey();
        if(tagFields.contains(key)) {
          metadataTags.put(key.toLowerCase(), tag.getValue());
        }
      }
      metadata.put("tags", metadataTags);

    } catch (Exception ex) {
      LOGGER.warn("Error while updating EC2 metadata - " + ex.getMessage() + ", ignoring");
    }
  }
}
