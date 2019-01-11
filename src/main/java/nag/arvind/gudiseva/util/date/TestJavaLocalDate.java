package nag.arvind.gudiseva.util.date;

import java.time.LocalDate;

public class TestJavaLocalDate {

    public static void main(String[] argv) {

        String dateStr = "2016-08-16";

        //default, ISO_LOCAL_DATE
        LocalDate localDate = LocalDate.parse(dateStr);

        System.out.println(localDate);

    }

}
