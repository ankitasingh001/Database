// 
// 



import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;



public class assign2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String inputPath="/home/ankita/Documents/db/Assignment-2/newsdata";
		String [] entities = {"modi","rahul","jaitley","sonia","lalu","nitish","farooq","sushma","tharoor","smriti","mamata","karunanidhi","kejriwal","sidhu","yogi","mayawati","akhilesh","chandrababu","chidambaram","fadnavis","uddhav","pawar"}; 

		String outputPath="/home/ankita/Documents/db/Assignment-2/output";
			    
		StructType structType = new StructType();
		structType = structType.add("word", DataTypes.StringType, false);
		structType = structType.add("id", DataTypes.StringType, false);
		ExpressionEncoder<Row> entityArticleEncoder = RowEncoder.apply(structType);
	    	
		SparkSession sparkSession = SparkSession.builder()
				.appName("assign2")		//Name of application
				.master("local")								//Run the application on local node
				.config("spark.sql.shuffle.partitions","2")		//Number of partitions
				.getOrCreate();
		
		//Read multi-line JSON from input files to dataset
		Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath); 
		
		Dataset<Row> xdataset =inputDataset.flatMap(new FlatMapFunction<Row,Row>(){
			public Iterator<Row> call(Row row) throws Exception {
				Row articleRow = getArticleFromJson(row);
				List<String> words = Arrays.asList((articleRow.getString(2)).split(" "));
				
				return words.stream()
					.map(word -> RowFactory.create(word, articleRow.get(3)))
					.collect(Collectors.toList())
					.iterator();

			}
		},entityArticleEncoder);
		
		/**
		 * Part-1 of the assignment 
		 */
		Dataset<Row> x_dataset_politics = xdataset.filter(xdataset.col("word").isInCollection(Arrays.asList(entities))).dropDuplicates();
		//x_dataset_politics.toJavaRDD().saveAsTextFile(outputPath);
		
		/**
		 * Part-2 of the assignment 
		 */
		Dataset<Row> count=x_dataset_politics.groupBy("word").count().as("word_count");
		Dataset<Row> count_filtered = count.filter(count.col("count").$greater(0));
		//count_filtered.toJavaRDD().saveAsTextFile(outputPath);

		/**
		 * Part-3 of the assignment 
		 */
		
		Dataset<Row> duplicatexdataset = x_dataset_politics.as("xdataset2").withColumnRenamed("word", "word1");
		Dataset<Row> jointdataset = x_dataset_politics.join(duplicatexdataset, x_dataset_politics.col("id").equalTo(duplicatexdataset.col("id")).and((duplicatexdataset.col("word1").$less(x_dataset_politics.col("word")))) , "inner").drop(duplicatexdataset.col("id")).drop(x_dataset_politics.col("id"));
		Dataset<Row> countpair=jointdataset.groupBy("word","word1").count().as("word_count");
		//jointdataset.show();
		//countpair.show();
		//countpair.toJavaRDD().saveAsTextFile(outputPath);
		
		/**
		 * Part-4 of the assignment 
		 */
		
		Dataset<Row> finaldataset = x_dataset_politics.join(duplicatexdataset,((duplicatexdataset.col("word1").$less(x_dataset_politics.col("word"))))).drop(duplicatexdataset.col("id")).drop(x_dataset_politics.col("id"));
		Dataset<Row> finaldataset_dd = finaldataset.drop("id").dropDuplicates();
		Dataset<Row> previous_dd = countpair.drop("count");
		//finaldataset_dd.show();
		Dataset<Row> result =  finaldataset_dd.unionAll(previous_dd).except(finaldataset_dd.intersect(previous_dd));
//		result.show();
//		result.toJavaRDD().saveAsTextFile(outputPath);
	}


private static Row getArticleFromJson(Row row) {
	String yearMonthPublished = ((String) row.getAs("date_published")).substring(0, 10);
	int year = Integer.parseInt(yearMonthPublished.substring(0,4));
	int month = Integer.parseInt(yearMonthPublished.substring(5, 7));
	String articleBody = (String) row.getAs("article_body");
	String id = row.getAs("article_id");
	articleBody = articleBody.toLowerCase().replaceAll("[^A-Za-z'\\-]", " "); // Remove all punctuation and convert to lower case
	articleBody = articleBody.replaceAll("( )+", " "); // Remove all double spaces
	articleBody = articleBody.trim();
	
	return RowFactory.create(year, month, articleBody, id);
}
}