package top.wxx.bs.algorithm.castle.original;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by zoltar on 2019/4/6.
 */

public class FileDataAccessor implements DataAccessor{
    private String fileName = "data/adultCASTLE.csv";
    private int n;

    public FileDataAccessor(int n){
        this.n = n;
    }

    @Override
    public List<Tuple> getAllTuple() {
        List<Tuple> res = null;
        try{
            res = Files.lines(Paths.get(fileName))
                    .limit(n)
                    .map(line -> lineToTuple(line))
                    .collect(Collectors.toList());
        } catch( IOException e ){
            throw new RuntimeException("some error occur : ", e);
        }
        return res;
    }

    private Tuple lineToTuple(String line){
        String[] fields = line.trim().split(",");
        List<String> trimedFields = Arrays.stream(fields).map(a -> a.trim()).collect(Collectors.toList());

        int pid = Integer.valueOf( trimedFields.get(0) );
        int receivedOrder = Integer.valueOf( trimedFields.get(1) );
        int age = Integer.valueOf( trimedFields.get(2) );
        int fhlweight = Integer.valueOf( trimedFields.get(3) );
        int education_num = Integer.valueOf( trimedFields.get(4) );
        int hour_per_week = Integer.valueOf( trimedFields.get(5) );
        String work_class = trimedFields.get(6);
        String education = trimedFields.get(7);
        String marital_status = trimedFields.get(8);
        String race = trimedFields.get(9);
        String gender = trimedFields.get(10);

        return new Tuple(pid, receivedOrder, age, fhlweight, education_num, hour_per_week,
                work_class, education, marital_status, race, gender);
    }

}
