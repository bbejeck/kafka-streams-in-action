package bbejeck.chapter_3.service;

import java.util.Date;


public interface SecurityDBService {

    static void saveRecord(Date date, String employeeId, String item) {
        System.out.println("Warning!! Found potential problem !! Saving transaction on "+date+" for "+employeeId+" item "+ item);
    }
}
