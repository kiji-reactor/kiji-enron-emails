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

package org.kiji.enronemail.bulkimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiPutter;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;


/**
 * This example bulk importer parses colon-delimited mappings of strings to integers.
 *
 * To write your own bulk importer class, change the type parameters to those of your key
 * and value, and override the produce method, using methods of <code>context</code> to write
 * your results.
 */
public class EmailBulkImporter extends KijiBulkImporter<LongWritable, Text> {
  /** {@inheritDoc} */
  @Override
  public void setup(KijiTableContext context) throws IOException {
    // TODO: Perform any setup you need here.
    super.setup(context);
  }

  /** {@inheritDoc} */
  @Override
   public void produce(LongWritable filePos, Text value, KijiTableContext context)
       throws IOException {

     System.out.println(value.toString());

     final String[] split = value.toString().split(":");
     final String rowKey = split[0];
     final int integerValue = Integer.parseInt(split[1]);

     final EntityId eid = context.getEntityId(rowKey);
     context.put(eid, "primitives", "int", integerValue);
   }

  public static void produceHelper(String message, KijiTable kijiTable, KijiPutter putter) throws IOException {
    final String[] lines = message.toString().split("\n");
    int progress = 0;
    Map<String, String> headers = Maps.newHashMap();
    Set<Character> headerContinueChars = Sets.newHashSet(' ', '\t');
    while ((progress < lines.length) && (lines[progress].length()>0)) {
      String[] fields = lines[progress].split(":", 2);

      if (fields.length == 2) {
        String key = fields[0];
        String value = fields[1].trim();

        // Look ahead for multi-line headers
        while ((progress+1 < lines.length) && headerContinueChars.contains(lines[progress+1])) {
          value = value + lines[progress+1];
          progress++;
        }

        if (value.length() > 0) {
          headers.put(key, value);
        }
      }
      progress++;
    }
    StringBuilder sb = new StringBuilder();
    while (progress < lines.length) {
      sb.append(lines[progress]);
      sb.append("\n");
      progress++;
    }
    String body = sb.toString();
    
    String messageId = headers.get("Message-ID");
    if (messageId.startsWith("<") && messageId.endsWith(">")) {
      messageId = messageId.substring(1, messageId.length()-2);
    }
    long ts = System.currentTimeMillis();
    EntityId eid = kijiTable.getEntityId(messageId);
    putter.put(eid, "info", "mid", ts, messageId);

    String date = headers.get("Date");
    if (null != date && !date.isEmpty()) {
      putter.put(eid, "info", "date", ts, date);
    }

    String from = headers.get("From");
    if (null != from && !from.isEmpty()) {
      putter.put(eid, "info", "from", ts, from);
    }

    String to = headers.get("To");
    if (null != to && !to.isEmpty()) {
      putter.put(eid, "info", "to", ts, to);
    }

    String subject = headers.get("Subject");
    if (null != subject && !subject.isEmpty()) {
      putter.put(eid, "info", "subject", ts, subject);
    }

    String cc = headers.get("X-cc" );
    if (null != cc && !cc.isEmpty()) {
      putter.put(eid, "info", "cc", ts, cc);
    }

    String bcc = headers.get("X-bcc");
    if (null != bcc && !bcc.isEmpty()) {
      putter.put(eid, "info", "cc", ts, bcc);
    }

    putter.put(eid, "info", "body", ts, body);
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup(KijiTableContext context) throws IOException {
    // TODO: Perform any cleanup you need here.
    super.cleanup(context);
  }

  public static long processDirectory(KijiTable table, KijiPutter putter,  File folder, String prefix) {
    long count = 0;
    if (folder.isDirectory()) {
      System.out.println("Processing: " + prefix + ": " + folder.toString());
      File[] files = folder.listFiles();
      for (int c=0; c<files.length; c++) {
        File file = files[c];
        count += processDirectory(table, putter, file, prefix +  "(" +c + "/" + files.length + ")");
      }
    } else {
      count++;
      BufferedReader br = null;

      try {
        StringBuilder sb = new StringBuilder();
        String currentLine;

        br = new BufferedReader(new FileReader(folder));

        while ((currentLine = br.readLine()) != null) {
          sb.append(currentLine);
          sb.append("\n");
        }
        String wholeFile = sb.toString();
        produceHelper(wholeFile, table, putter);
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          if (br != null) br.close();
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    }
    return count;
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Must pass in a Maildir directory");
      System.exit(1);
    }
    String kijiURIString = args[0];
    String path = args[1];
    KijiURI kijiURI = KijiURI.newBuilder(kijiURIString).build();
    try {
      Kiji kiji = Kiji.Factory.open(kijiURI);
      KijiTable kijiTable = kiji.openTable(kijiURI.getTable());
      KijiTableWriter kijiTableWriter = kijiTable.openTableWriter();

      File folder = new File(path);
      long count = processDirectory(kijiTable, kijiTableWriter, folder, "");
      System.out.println("Count: " + count);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
