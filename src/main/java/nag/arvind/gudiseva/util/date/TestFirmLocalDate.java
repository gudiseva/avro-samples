package nag.arvind.gudiseva.util.date;


import firm.type.LocalDate;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TestFirmLocalDate {

    public static void main(String[] argv) {

        String inputDate = "2016-08-16";
        System.out.println("Input Date: " + inputDate);

        try {

            //string date format
            SimpleDateFormat strPattern = new SimpleDateFormat("yyyy-MM-dd");

            //value date format
            SimpleDateFormat valPattern = new SimpleDateFormat("yyyyMMdd");

            Date convDate = strPattern.parse(inputDate);
            String dateStr = valPattern.format(convDate);
            System.out.println("Converted Date: " + dateStr);

            LocalDate.Builder localDateBuilder = LocalDate.newBuilder();

            //default, ISO_LOCAL_DATE
            localDateBuilder.setValue(Integer.valueOf(dateStr));
            LocalDate localDate = localDateBuilder.build();

            Integer dateVal = (Integer) localDate.get("value");
            System.out.println("Local Date: " + dateVal);

            Date dateLoc = valPattern.parse(dateVal.toString());
            String parseDate = strPattern.format(dateLoc);
            System.out.println("Parsed Date: " + parseDate);

        } catch (Exception e) {
            System.out.println("Invalid Date: " + e.getMessage());
        }

    }

}
