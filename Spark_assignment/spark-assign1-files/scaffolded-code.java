import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Cooccurence_Count {

	public static void main(String[] args) {

		// Input dir - should contain all input json files
		// Change all paths to your local paths before running this code
		String inputPath = "/home/balaji/IIT B/courses/Database Systems/assignment 3/newsdata"; // Use absolute paths

		String entityPath = "/home/balaji/IIT B/courses/Database Systems/assignment 3/entity.txt";

		// Ouput dir - this directory will be created by spark. Delete this directory
		// between each run
		String outputPathTemp = "/home/balaji/IIT B/courses/Database Systems/assignment 3/output_sample_midway" + (new Date()); // Use absolute paths
		String outputPath = "/home/balaji/IIT B/courses/Database Systems/assignment 3/output_ass" + (new Date()); // Use absolute paths

		// We have provided you several StructTypes (schemas) 
		//   along with ExpressionEncoders, to simplify your life.

		StructType structType = new StructType();
		// contains year month and date of the article
		structType = structType.add("year", DataTypes.IntegerType, false); // false => not nullable
		structType = structType.add("month", DataTypes.IntegerType, false);
		structType = structType.add("word", DataTypes.StringType, false);
		structType = structType.add("id", DataTypes.StringType, false);
		ExpressionEncoder<Row> articleRowEncoder = RowEncoder.apply(structType);

		StructType joinedType = new StructType();
		// contains year month and date of the article
		joinedType = joinedType.add("year", DataTypes.IntegerType, false);
		joinedType = joinedType.add("month", DataTypes.IntegerType, false);
		joinedType = joinedType.add("first_word", DataTypes.StringType, false);
		joinedType = joinedType.add("second_word", DataTypes.StringType, false);
		ExpressionEncoder<Row> joinedRowEncoder = RowEncoder.apply(joinedType);

		StructType intervalType = new StructType();
		intervalType = intervalType.add("first_entity", DataTypes.StringType, false);
		// contains year month and date of the article
		intervalType = intervalType.add("second_entity", DataTypes.StringType, false); // false => not nullable
		intervalType = intervalType.add("interval_start", DataTypes.StringType, false);
		intervalType = intervalType.add("interval_end", DataTypes.StringType, false);
		intervalType = intervalType.add("count", DataTypes.LongType, false);
		ExpressionEncoder<Row> intervalRowEncoder = RowEncoder.apply(intervalType);


		SparkSession sparkSession = SparkSession.builder().appName("Month wise news articles") // Name of application
			.master("local") // Run the application on local node
			.config("spark.sql.shuffle.partitions", "2") // Number of partitions
			.getOrCreate();

		// Read multi-line JSON from input files to dataset
		Dataset<Row> inputDataset = sparkSession.read().option("multiLine", true).json(inputPath);

		JavaRDD<String> entityRdd = sparkSession.read().textFile(entityPath).javaRDD();
		Set<String> entitySet = new HashSet<String>(entityRdd.collect());


		inputDataset.printSchema();

		// collect all the words in each article into a data set
		// Create a Dataset  articleEntityDataset(year, month, word, ID), by processing inputDataset.
     		//  Hint:  Use a map and a flatmap, using functions getArticleFromJson() and getWordsFromArticle()
     		// YOUR CODE HERE 		

		// remove/filter unnecessary words and keep only entity names
		// YOUR CODE HERE	


		Dataset<Row> cloneArticleEntity = articleEntityDataset.as("ae2");
		// self join entities if id is equal and words are not equal. The clone of the existing dataset created above 
		// can be used as the other relation in the join.
		// YOUR CODE HERE	

		joineDataset.printSchema();


		//drop the id column and the duplicate year, month columns so as to aggregate later based on months 
		// YOUR CODE HERE	

		joineDataset.printSchema();		

		// aggregate the count of each pair for every month for this you have to group by year and month
		// YOUR CODE HERE	

		joineDataset.toJavaRDD().saveAsTextFile(outputPathTemp);

		// sort on the basis of first_word, second_word and count
		// YOUR CODE HERE	

		// groupby based on first_word and second_word and aggregate months and count into a list
		// YOUR CODE HERE	

		//joineDataset.show();
		joineDataset.printSchema();

		// call the function with createIntervalsForARow with row as argument
		// make sure that the schema  of row matches with the schema required by the function.
		// YOUR CODE HERE	

		// store the result in a file
		joineDataset.toJavaRDD().saveAsTextFile(outputPath);
	}

	/*
	 * The schema of the row when calling this function should be as follows
	 *	root
	 *	|-- first_word: string (nullable = false)
	 *	|-- second_word: string (nullable = false)
	 *	|-- collect_list(year): array (nullable = true)
	 *	|    |-- element: integer (containsNull = true)
	 *	|-- collect_list(month): array (nullable = true)
	 *	|    |-- element: integer (containsNull = true)
	 *	|-- collect_list(count): array (nullable = true)
	 *	|    |-- element: long (containsNull = true)
	 */
	// Given a row this function will create intervals for the months associated with this
	// The output will be an iterator to the list of rows whose schema is same as the intervalRowEncoder
	private static Iterator<Row> createIntervalsForARow(Row row) {
		List<Integer> years = new ArrayList<>(row.getList(2));
		List<Integer> months = new ArrayList<>(row.getList(3));
		List<Long> countList = new ArrayList<>(row.getList(4));
		Map<String, Long> monthToCountMap = new HashMap<>();

		List<String> monthStrings = new ArrayList<>();
		for (int i = 0; i < months.size(); i++) {
			monthStrings.add(Integer.toString(years.get(i)) + "-" + ("00" + Integer.toString(months.get(i))).substring(Integer.toString(months.get(i)).length()));
			monthToCountMap.put(monthStrings.get(i), countList.get(i));
		}
		java.util.Collections.sort(monthStrings);

		List<Row> intervalRows = new ArrayList<>();
		for (int i = 0; i< monthStrings.size();) {
			String startMonthString = monthStrings.get(i);
			String endMonthString = monthStrings.get(i);
			long count = monthToCountMap.get(startMonthString);
			i++;
			if (monthStrings.size() > 1) {
				while (i < monthStrings.size()) {
					int current_year = Integer.parseInt(monthStrings.get(i).substring(0, 4));
					int current_month = Integer.parseInt(monthStrings.get(i).substring(5,7));

					int prev_year = Integer.parseInt(monthStrings.get(i-1).substring(0, 4));
					int prev_month = Integer.parseInt(monthStrings.get(i-1).substring(5,7));

					if ((prev_month == current_month - 1 && prev_year == current_year) || 
							(current_month == 1 && prev_month == 12 && prev_year == current_year - 1)) {
						endMonthString = monthStrings.get(i);
						count += monthToCountMap.get(endMonthString);
						i++;
					}
					else {
						break;
					}
				}
				intervalRows.add(RowFactory.create(row.get(0), row.get(1), startMonthString, endMonthString, Long.toString(count)));
			}
			else {
				intervalRows.add(RowFactory.create(row.get(0), row.get(1), startMonthString, endMonthString, Long.toString(count)));
			}
		}
		return intervalRows.iterator();
	}
}
private static Iterator<Row> getWordsFromArticle(Row row) {
	List<String> words = Arrays.asList((row.getString(2)).split(" "));
	// we could have used words.parallelStream() if we want to parallelize the processing of the word stream.
	return words.stream()
		// The map and collect functions used here are from the java library and not from spark.
		// Java collections framework provides you these functions on Objects of Stream<T> class.
		// You can get streams out of classes in Collection framework like List, Set, Map etc.
		.map(word -> RowFactory.create(row.getInt(0), row.getInt(1), word, row.get(3)))
		.collect(Collectors.toList())
		.iterator();
}

private static Row getArticleFromJson(Row row) {
	String yearMonthPublished = ((String) row.getAs("date_published")).substring(0, 10);
	int year = Integer.parseInt(yearMonthPublished.substring(0,4));
	int month = Integer.parseInt(yearMonthPublished.substring(5, 7));
	String articleBody = (String) row.getAs("article_body");
	String id = row.getAs("source_article_link");
	articleBody = articleBody.toLowerCase().replaceAll("[^A-Za-z'\\-]", " "); // Remove all punctuation and convert
	// to lower case
	articleBody = articleBody.replaceAll("( )+", " "); // Remove all double spaces
	articleBody = articleBody.trim();
	return RowFactory.create(year, month, articleBody, id);
}
