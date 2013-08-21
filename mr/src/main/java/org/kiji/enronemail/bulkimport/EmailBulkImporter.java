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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.schema.*;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * This example bulk importer parses colon-delimited mappings of strings to integers.
 * <p/>
 * To write your own bulk importer class, change the type parameters to those of your key
 * and value, and override the produce method, using methods of <code>context</code> to write
 * your results.
 */
public class EmailBulkImporter {
    private final static DateFormat DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z (z)");

    private static final String FAMILY = "sent_messages";

    public final static class SystemFilenameFilter implements FilenameFilter {

        Set<String> ignoredFiles = new HashSet<String>();

        public SystemFilenameFilter() {
            ignoredFiles.add(".DS_Store");
        }

        @Override
        public boolean accept(File dir, String name) {
            return !ignoredFiles.contains(name);
        }
    }




    public static void produceHelper(String message, KijiTable kijiTable, KijiPutter putter) throws IOException {
        final String[] lines = message.split("\n");
        int progress = 0;
        Map<String, String> headers = Maps.newHashMap();
        Set<Character> headerContinueChars = Sets.newHashSet(' ', '\t');
        while ((progress < lines.length) && (lines[progress].length() > 0)) {
            String[] fields = lines[progress].split(":", 2);

            if (fields.length == 2) {
                String key = fields[0];
                String value = fields[1].trim();

                // Look ahead for multi-line headers
                while ((progress + 1 < lines.length) && headerContinueChars.contains(lines[progress + 1])) {
                    value = value + lines[progress + 1];
                    progress++;
                }

                // Only use the first occurrence of a particular header.
                if (value.length() > 0 && !headers.containsKey(key)) {
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

        String dateStr = headers.get("Date");
        Date date;
        try {
            date = DATE_FORMAT.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new IOException(e);
        }

        String body = sb.toString();
        // Contract contractions.
        body = body.replaceAll("'", "");

        // Filter out any non-textual characters
        body = body.replaceAll("\\W", " ");

        // Compress whitespace
        body = body.replaceAll(" +", " ");

        String messageId = headers.get("Message-ID");
        if (messageId.startsWith("<") && messageId.endsWith(">")) {
            messageId = messageId.substring(1, messageId.length() - 2);
        }

        String from = headers.get("From");
        long ts = date.getTime();

        // Use the From line and the timestamp of the email as the rowkey.
        EntityId eid = kijiTable.getEntityId(from);
        putter.put(eid, FAMILY, "mid", ts, messageId);

        putter.put(eid, FAMILY, "date", ts, date.getTime());

        if (null != from && !from.isEmpty()) {
            putter.put(eid, FAMILY, "from", ts, from);
        }

        String to = headers.get("To");
        if (null != to && !to.isEmpty()) {
            putter.put(eid, FAMILY, "to", ts, to);
        }

        String subject = headers.get("Subject");
        if (null != subject && !subject.isEmpty()) {
            putter.put(eid, FAMILY, "subject", ts, subject);
        }

        String cc = headers.get("X-cc");
        if (null != cc && !cc.isEmpty()) {
            putter.put(eid, FAMILY, "cc", ts, cc);
        }

        String bcc = headers.get("X-bcc");
        if (null != bcc && !bcc.isEmpty()) {
            putter.put(eid, FAMILY, "cc", ts, bcc);
        }

        putter.put(eid, FAMILY, "body", ts, body);
    }



    public static long processDirectory(KijiTable table, KijiPutter putter, File folder, String prefix) {
        long count = 0;
        if (folder.isDirectory()) {
            System.out.println("Processing: " + prefix + ": " + folder.toString());
            File[] files = folder.listFiles(new SystemFilenameFilter());
            for (int c = 0; c < files.length; c++) {
                File file = files[c];
                count += processDirectory(table, putter, file, prefix + "(" + c + "/" + files.length + ")");
            }
        } else {
            count++;
            BufferedReader br = null;
//      System.out.println("File: " + folder.toString());

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
