import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockHousingCorrelation {

    public static class CorrelationMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();
        private String ticker;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Extract ticker symbol from filename e.g. "AAPL.csv" -> "AAPL"
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            ticker = filename.replace(".csv", "");
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");

            // Stock CSV format: Date, Adj Close, Close, High, Low, Open, Volume
            // Skip header row
            if (parts[0].equalsIgnoreCase("Date")) return;

            // Need at least 3 columns and Close price at index 2 must not be empty
            if (parts.length >= 3 && !parts[2].trim().isEmpty()) {
                String dateStr = parts[0].trim();
                String closePriceStr = parts[2].trim();

                try {
                    double closePrice = Double.parseDouble(closePriceStr);
                    if (dateStr.length() >= 7) {
                        String yearMonth = dateStr.substring(0, 7); // YYYY-MM
                        outKey.set(ticker);
                        outValue.set("STOCK," + yearMonth + "," + closePrice);
                        context.write(outKey, outValue);
                    }
                } catch (NumberFormatException e) {
                    // Ignore rows with missing/invalid close price
                }
            }
        }
    }

    public static class CorrelationReducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, Double> housingData = new HashMap<>();
        private Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load housing data directly from HDFS before any reduce() calls
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path housingPath = new Path("/input/correlation/CSUSHPINSA.csv");
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(housingPath)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2 && !parts[0].trim().equalsIgnoreCase("observation_date")) {
                    try {
                        String yearMonth = parts[0].trim().substring(0, 7);
                        double indexVal = Double.parseDouble(parts[1].trim());
                        housingData.put(yearMonth, indexVal);
                    } catch (NumberFormatException e) {
                        // Ignore parse errors
                    }
                }
            }
            br.close();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, List<Double>> stockPrices = new HashMap<>();
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length == 3 && parts[0].equals("STOCK")) {
                    String ym = parts[1];
                    double price = Double.parseDouble(parts[2]);
                    stockPrices.computeIfAbsent(ym, k -> new ArrayList<>()).add(price);
                }
            }

            // 1. Monthly Average
            Map<String, Double> monthlyAvg = new HashMap<>();
            for (Map.Entry<String, List<Double>> entry : stockPrices.entrySet()) {
                double sum = 0;
                for (double p : entry.getValue()) sum += p;
                monthlyAvg.put(entry.getKey(), sum / entry.getValue().size());
            }

            // 2. Sort chronologically
            List<String> sortedMonths = new ArrayList<>(monthlyAvg.keySet());
            Collections.sort(sortedMonths);

            // 3. Month-over-Month % Change
            List<Double> stockPctChanges = new ArrayList<>();
            List<Double> housingPctChanges = new ArrayList<>();

            for (int i = 1; i < sortedMonths.size(); i++) {
                String prevMonth = sortedMonths.get(i - 1);
                String currMonth = sortedMonths.get(i);

                if (!housingData.containsKey(prevMonth) || !housingData.containsKey(currMonth)) continue;

                double prevStock = monthlyAvg.get(prevMonth);
                double currStock = monthlyAvg.get(currMonth);
                double prevHousing = housingData.get(prevMonth);
                double currHousing = housingData.get(currMonth);

                if (prevStock == 0 || prevHousing == 0) continue;

                stockPctChanges.add((currStock - prevStock) / prevStock);
                housingPctChanges.add((currHousing - prevHousing) / prevHousing);
            }

            // 4. Pearson Correlation
            int n = stockPctChanges.size();
            if (n >= 12) {
                double sumX = 0,
                    sumY = 0,
                    sumXY = 0,
                    sumX2 = 0,
                    sumY2 = 0;
                for (int i = 0; i < n; i++) {
                    double x = stockPctChanges.get(i);
                    double y = housingPctChanges.get(i);
                    sumX += x;
                    sumY += y;
                    sumXY += x * y;
                    sumX2 += x * x;
                    sumY2 += y * y;
                }

                double numerator = (n * sumXY) - (sumX * sumY);
                double denominatorSq = ((n * sumX2) - (sumX * sumX)) * ((n * sumY2) - (sumY * sumY));

                if (denominatorSq > 0) {
                    double correlation = numerator / Math.sqrt(denominatorSq);
                    // Output: Ticker \t Months_Compared \t Pearson_Correlation
                    outValue.set(n + "\t" + String.format("%.6f", correlation));
                    context.write(key, outValue);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: StockHousingCorrelation <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("mapreduce.job.reduces", 1);
        conf.set("mapreduce.framework.name", "yarn");

        Job job = Job.getInstance(conf, "Stock and Housing Pearson Correlation");
        job.setJarByClass(StockHousingCorrelation.class);
        job.setMapperClass(CorrelationMapper.class);
        job.setReducerClass(CorrelationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
