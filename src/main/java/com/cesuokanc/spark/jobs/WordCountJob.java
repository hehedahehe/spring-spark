package com.cesuokanc.spark.jobs;


import com.cesuokanc.spark.SparkConfiguration;
import com.cesuokanc.spark.beans.Count;
import com.cesuokanc.spark.beans.Word;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * @desc
 * @author lirb
 * @datetime 2017/12/7,16:46
 */
@Component
public class WordCountJob {

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    SparkConfiguration applicationConfiguration;

    public List<Count> count() {
        //String input = "hello world hello hello hello";
        String input = applicationConfiguration.getInputString();
        String[] _words = input.split(" ");
        List<Word> words = Arrays.stream(_words).map(Word::new).collect(Collectors.toList());
        Dataset<Row> dataFrame = sparkSession.createDataFrame(words, Word.class);
        dataFrame.show();
        //StructType structType = dataFrame.schema();

        RelationalGroupedDataset groupedDataset = dataFrame.groupBy(col("word"));
        groupedDataset.count().show();
        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).count();
        return rows.stream().map(new Function<Row, Count>() {
            @Override
            public Count apply(Row row) {
                return new Count(row.getString(0), row.getLong(1));
            }
        }).collect(Collectors.toList());
    }
}
