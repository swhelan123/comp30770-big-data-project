import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockHousingCorrelation {

    public static class CorrelationMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");

            // Heuristic to separate housing data from stock data
            if (parts.length >= 7) {
                // Stock Data (Ticker, Date, Open, High, Low, Close, Adj Close, Volume)
                String ticker = parts[0];
                if (ticker.equalsIgnoreCase("Ticker") || ticker.equalsIgnoreCase("Symbol")) {
                    return; // Skip header
                }

                String dateStr = parts[1];
                // Assuming Close price is at index 5 (or 3 depending on dataset).
                // We will try index 5 first, falling back to 3 if needed.
                String closePriceStr = parts.length > 5 ? parts[5] : parts[3];

                try {
                    double closePrice = Double.parseDouble(closePriceStr);
                    if (dateStr.length() >= 7) {
                        String yearMonth = dateStr.substring(0, 7); // Extract YYYY-MM
                        outKey.set(ticker);
                        outValue.set("STOCK," + yearMonth + "," + closePrice);
                        context.write(outKey, outValue);
                    }
                } catch (NumberFormatException e) {
                    // Ignore parse errors (e.g., missing data)
                }
            } else if (parts.length == 2) {
                // Housing Data (DATE, CSUSHPINSA)
                String dateStr = parts[0];
                if (dateStr.toUpperCase().contains("DATE")) {
                    return; // Skip header
                }

                try {
                    double indexVal = Double.parseDouble(parts[1]);
                    if (dateStr.length() >= 7) {
                        String yearMonth = dateStr.substring(0, 7);
                        // Use 000_HOUSING to ensure it gets sorted lexicographically before any Ticker
                        outKey.set("000_HOUSING");
                        outValue.set("HOUSING," + yearMonth + "," + indexVal);
                        context.write(outKey, outValue);
                    }
                } catch (NumberFormatException e) {
                    // Ignore parse errors
                }
            }
        }
    }

    public static class CorrelationReducer extends Reducer<Text, Text, Text, Text> {

        // Caches the housing data. Since 000_HOUSING comes first, this will be populated early.
        private Map<String, Double> housingData = new HashMap<>();
        private Text outValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String currentKey = key.toString();

            if (currentKey.equals("000_HOUSING")) {
                for (Text val : values) {
                    String[] parts = val.toString().split(",");
                    if (parts.length == 3 && parts[0].equals("HOUSING")) {
                        housingData.put(parts[1], Double.parseDouble(parts[2]));
                    }
                }
                return;
            }

            // Processing Stock Data for a specific Ticker
            Map<String, List<Double>> stockPrices = new HashMap<>();
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length == 3 && parts[0].equals("STOCK")) {
                    String ym = parts[1];
                    double price = Double.parseDouble(parts[2]);
                    stockPrices.computeIfAbsent(ym, k -> new ArrayList<>()).add(price);
                }
            }

            // 1. Temporal Aggregation: Calculate Monthly Average
            Map<String, Double> monthlyAvg = new HashMap<>();
            for (Map.Entry<String, List<Double>> entry : stockPrices.entrySet()) {
                double sum = 0;
                for (double p : entry.getValue()) {
                    sum += p;
                }
                monthlyAvg.put(entry.getKey(), sum / entry.getValue().size());
            }

            // 2. Sort months chronologically
            List<String> sortedMonths = new ArrayList<>(monthlyAvg.keySet());
            Collections.sort(sortedMonths);

            // 3. Feature Engineering: Month-over-Month Percentage Change
            List<Double> stockPctChanges = new ArrayList<>();
            List<Double> housingPctChanges = new ArrayList<>();

            for (int i = 1; i < sortedMonths.size(); i++) {
                String prevMonth = sortedMonths.get(i - 1);
                String currMonth = sortedMonths.get(i);

                // Both datasets must have data for the previous and current month
                if (!housingData.containsKey(prevMonth) || !housingData.containsKey(currMonth)) {
                    continue;
                }

                double prevStock = monthlyAvg.get(prevMonth);
                double currStock = monthlyAvg.get(currMonth);
                double prevHousing = housingData.get(prevMonth);
                double currHousing = housingData.get(currMonth);

                // Prevent division by zero
                if (prevStock == 0 || prevHousing == 0) {
                    continue;
                }

                double stockPct = (currStock - prevStock) / prevStock;
                double housingPct = (currHousing - prevHousing) / prevHousing;

                stockPctChanges.add(stockPct);
                housingPctChanges.add(housingPct);
            }

            // 4. Statistical Analysis: Pearson Correlation
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
                    // Output format: Ticker \t Months_Correlated \t Pearson_Correlation
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
        // Force exactly 1 reducer so that "000_HOUSING" key processes housing data
        // before any tickers are evaluated within the same JVM instance.
        conf.setInt("mapreduce.job.reduces", 1);

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
