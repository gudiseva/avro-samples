package nag.arvind.gudiseva.avro.specific;

import com.example.Customer;
import firm.transaction.versioned.TransactionKey;
import firm.type.LocalDate;
import nag.arvind.gudiseva.util.date.StringToLocalDate;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TransactionRecordExamples {

    private TransactionKey.Builder builder;

    public static void main(String[] args) throws ParseException {

        // step 1: create specific record
        TransactionKey.Builder transactionKeyBuilder = TransactionKey.newBuilder();
        transactionKeyBuilder.setSource("PANO");
        transactionKeyBuilder.setId("5464");

        // set transaction date
        String transactionDate = "2018-12-07";

        //string date format
        SimpleDateFormat strPattern = new SimpleDateFormat("yyyy-MM-dd");

        //value date format
        SimpleDateFormat valPattern = new SimpleDateFormat("yyyyMMdd");

        Date convDate = strPattern.parse(transactionDate);
        String dateStr = valPattern.format(convDate);

        LocalDate.Builder localDateBuilder = LocalDate.newBuilder();

        //default, ISO_LOCAL_DATE
        localDateBuilder.setValue(Integer.valueOf(dateStr));
        LocalDate localDate = localDateBuilder.build();

        transactionKeyBuilder.setTransactionDate(localDate);
        transactionKeyBuilder.setVersion(0);

        TransactionKey transactionKey = transactionKeyBuilder.build();
        System.out.println(transactionKey);

        // step 2: write to file
        final DatumWriter<TransactionKey> datumWriter = new SpecificDatumWriter<>(TransactionKey.class);

        try (DataFileWriter<TransactionKey> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(transactionKey.getSchema(), new File("transactionKey-specific.avro"));
            dataFileWriter.append(transactionKey);
            System.out.println("successfully wrote transactionKey-specific.avro");
        } catch (IOException e){
            e.printStackTrace();
        }

        // step 3: read from file
        final File file = new File("transactionKey-specific.avro");
        final DatumReader<TransactionKey> datumReader = new SpecificDatumReader<>(TransactionKey.class);
        final DataFileReader<TransactionKey> dataFileReader;
        try {
            System.out.println("Reading our specific record");
            dataFileReader = new DataFileReader<>(file, datumReader);
            while (dataFileReader.hasNext()) {
                // step 4: interpret
                TransactionKey readTransactionKey = dataFileReader.next();
                System.out.println(readTransactionKey.toString());
                Date dateValue = valPattern.parse(readTransactionKey.getTransactionDate().get("value").toString());
                String parseDate = strPattern.format(dateValue);
                System.out.println("Transaction Date: " + parseDate);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
