package nag.arvind.gudiseva.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class TransactionRecordExamples {

    public static void main(String[] args) {

        // step 0: define schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "    \"type\": \"record\",\n" +
                "    \"name\": \"TransactionKey\",\n" +
                "    \"namespace\": \"firm.transaction.versioned\",\n" +
                "    \"doc\": \"The key which uniquely identifies transaction\",\n" +
                "    \"fields\": [{\n" +
                "        \"name\": \"source\",\n" +
                "        \"type\": \"string\",\n" +
                "        \"doc\": \"transaction source\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"name\": \"id\",\n" +
                "        \"type\": \"string\",\n" +
                "        \"doc\": \"transaction id\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"name\": \"transactionDate\",\n" +
                "        \"type\": {\n" +
                "            \"type\": \"record\",\n" +
                "            \"name\": \"LocalDate\",\n" +
                "            \"namespace\": \"firm.type\",\n" +
                "            \"doc\": \"Type firm.type.LocalDate should be used when you want to serialize & deserialize a java.time.LocalDate instance using avro. \\n\\t *  On the wire that data is serialized/deserialized (Date is converted into yyyyMMdd and treated as an in on the wire) with the below schema. \\n\\t *  The firm.schema.common.avro.Record has the required api that convert the java.time.LocalDate to and from this schema.\",\n" +
                "            \"fields\": [{\n" +
                "                \"name\": \"value\",\n" +
                "                \"type\": \"int\",\n" +
                "                \"doc\": \"localDate in yyyyMMdd format.\",\n" +
                "                \"default\": 19000101\n" +
                "            }],\n" +
                "            \"logicalType\": \"LocalDate\"\n" +
                "        },\n" +
                "        \"doc\": \"transaction date (the date when transaction was generated)\"\n" +
                "    },\n" +
                "    {\n" +
                "       \"name\": \"version\",\n" +
                "       \"type\": \"int\",\n" +
                "       \"doc\": \"transaction version\"\n" +
                "    }],\n" +
                "    \"root\": \"true\"\n" +
                "}");

        // step 1: create a generic record
        GenericRecordBuilder transactionBuilder = new GenericRecordBuilder(schema);
        transactionBuilder.set("source", "PANO");
        transactionBuilder.set("id", "5464");
        transactionBuilder.set("transactionDate", "2018-12-07");
        transactionBuilder.set("version", "0");
        GenericData.Record transactionRecord = transactionBuilder.build();

        System.out.println(transactionRecord);

        // step 2: write that generic record to a file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.setFlushOnEveryBlock(true);
            dataFileWriter.setSyncInterval(32);
            File avroFile = File.createTempFile("transaction-generic", ".avro");
            dataFileWriter.create(schema, avroFile);

            GenericRecord datum = new GenericData.Record(schema);
            datum.put("source", "PANO");
            datum.put("id", "5464");
            datum.put("transactionDate", "2018-12-07");
            datum.put("version", "0");

            dataFileWriter.append(datum);


            //dataFileWriter.create(transactionRecord.getSchema(), new File("transaction-generic.avro"));
            //dataFileWriter.append(transactionRecord);
            System.out.println("Written transaction-generic.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }
        // step 3: read a generic record from a file

        // step 4: interpret as a generic record


    }
}
