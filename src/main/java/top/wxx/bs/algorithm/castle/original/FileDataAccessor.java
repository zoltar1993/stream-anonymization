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
                    .map(line -> DataConvertor.lineToTuple(line))
                    .collect(Collectors.toList());
        } catch( IOException e ){
            throw new RuntimeException("some error occur : ", e);
        }
        return res;
    }


}
