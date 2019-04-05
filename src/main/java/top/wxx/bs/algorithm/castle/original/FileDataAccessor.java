package top.wxx.bs.algorithm.castle.original;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by zoltar on 2019/4/6.
 */

public class FileDataAccessor implements DataAccessor{
    private String fileName = "data/adultCASTLE.csv";

    @Override
    public List<Tuple> getAllTuple(int n) {
        List<Tuple> res = null;
        try{
            res = Files.lines(Paths.get(fileName))
                    .map(line -> lineToTuple(line))
                    .collect(Collectors.toList());
        } catch( IOException e ){
            throw new RuntimeException("some error occur : ", e);
        }
        return res;
    }

    private Tuple lineToTuple(String line){
        String[] fields = line.trim().split(",");

        int pid = Integer.valueOf( fields[0] );
        int receivedOrder = Integer.valueOf( fields[1] );
        int age = Integer.valueOf( fields[2] );
        int fhlweight = Integer.valueOf( fields[3] );
        int education_num = Integer.valueOf( fields[4] );
        int hour_per_week = Integer.valueOf( fields[5] );
        String work_class = fields[6];
        String education = fields[7];
        String marital_status = fields[8];
        String race = fields[6];
        String gender = fields[6];

        return new Tuple(pid, receivedOrder, age, fhlweight, education_num, hour_per_week,
                work_class, education, marital_status, race, gender);
    }

}
