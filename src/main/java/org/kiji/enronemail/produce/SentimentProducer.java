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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.util.ResourceUtils;

/**
 * This producer copies data from the column "family:qualifier" to the column
 * "family:outputqualifier" without modification, assuming both are encoded as bytes.
 */
public class SentimentProducer extends KijiProducer {
  private static final Logger LOG = LoggerFactory.getLogger(SentimentProducer.class);

  private KijiColumnName mInputColumn = new KijiColumnName("info:body");
  private KijiColumnName mOutputColumn = new KijiColumnName("features:sentiment");
  private Schema schema = Schema.create(Schema.Type.FLOAT);
  public Map<String, Integer> affinityMap;

  /** {@inheritDoc} */
  @Override
  public void setup(KijiContext context) {

    //FIXME I should use a kv store here, but I'm lazy.
    affinityMap = Maps.newHashMap();
    File file = new File("/home/lsheng/project/kijienronemail/AFINN-111.txt");
    BufferedReader br = null;
    try {
      String currentLine;

      br = new BufferedReader(new FileReader(file));

      while ((currentLine = br.readLine()) != null) {
        String[] fields = currentLine.split("\t");
        String word = fields[0];
        Integer value = Integer.parseInt(fields[1]);
        affinityMap.put(word, value);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        ResourceUtils.closeOrLog(br);
      }
    }
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
    String body = input.getMostRecentValue(mInputColumn.getFamily(), mInputColumn.getQualifier()).toString();
    // replace all non word characters with spaces
    body = body.replaceAll("\\W", " ");
    body = body.toLowerCase();
    String[] words = body.split(" ");
    float score = 0.0f;
    float numWords = 0.0f;
    for (String word : words) {
      word = word.trim();
      if (affinityMap.containsKey(word)) {
        numWords++;
        score += affinityMap.get(word);
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
    SentimentProducer kp = new SentimentProducer();
    kp.setup(null);
    System.out.println(kp.affinityMap);
  }
}
