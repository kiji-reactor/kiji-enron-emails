/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.enronemail.produce;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.lib.TextFileKeyValueStore;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

/**
 * This producer copies data from the column "family:qualifier" to the column
 * "family:outputqualifier" without modification, assuming both are encoded as bytes.
 */
public class SentimentProducer extends KijiProducer {
  private static final Logger LOG = LoggerFactory.getLogger(SentimentProducer.class);

  private KijiColumnName mInputColumn = new KijiColumnName("info:body");
  private KijiColumnName mOutputColumn = new KijiColumnName("features:sentiment");
  private Schema schema = Schema.create(Schema.Type.FLOAT);

  /** {@inheritDoc} */
  @Override
  public void setup(KijiContext context) {
  }

  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    // Note that it's ok to specify a path to an HDFS dir, not just one file.
    return RequiredStores.just("sentiment", TextFileKeyValueStore.builder()
        .withDelimiter("\t")
        .withInputPath(new Path("/tmp/AFINN-111.txt"))
        .withDistributedCache(false)
        .build());
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(Integer.MAX_VALUE)
        .add(mInputColumn.getFamily(), mInputColumn.getQualifier());
    return builder.build();
  }

  /** {@inheritDoc} */
  @Override
  public String getOutputColumn() {
    return mOutputColumn.toString();
  }

  /** {@inheritDoc} */
  @Override
  public void produce(KijiRowData input, ProducerContext context)
      throws IOException {
    KeyValueStoreReader<String, String> sentimentStore = context.getStore("sentiment");
    String body = input.getMostRecentValue(mInputColumn.getFamily(), mInputColumn.getQualifier()).toString();
    // replace all non word characters with spaces
    body = body.replaceAll("\\W", " ");
    body = body.toLowerCase();
    String[] words = body.split(" ");
    float score = 0.0f;
    float numWords = 0.0f;
    for (String word : words) {
      word = word.trim();
      if (sentimentStore.containsKey(word)) {
        numWords++;
        score += Float.valueOf(sentimentStore.get(word));
      }
    }
    long timestamp = System.currentTimeMillis();
    if(numWords > 0) {
      score = score / numWords;
      context.put(timestamp, score);
    } else {
      context.put(timestamp, 0.0f);
    }
  }

  public static void main(String[] args) {
  }
}
