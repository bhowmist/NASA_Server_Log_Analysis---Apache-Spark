# NASA Apache Web Server Log Analysis --Apache Spark

#Data set from NASA Kennedy Space Center web server in Florida
#The full data set is freely available at http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html, and it contains all HTTP requests for two months.

# Specify path to downloaded log file
import sys
import os
from pyspark.sql.functions import split, regexp_extract

log_file_path = 'dbfs:/' + os.path.join('databricks-datasets', 'cs100', 'lab2', 'data-001', 'apache.access.log.PROJECT')

#Loading the log file
# Specify path to downloaded log file
import sys
import os

log_file_path = 'dbfs:/' + os.path.join('databricks-datasets', 'cs100', 'lab2', 'data-001', 'apache.access.log.PROJECT')
base_df = sqlContext.read.text(log_file_path)
# Let's look at the schema
base_df.printSchema()

#Parsing the log file
split_df = base_df.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                          regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
                          regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('path'),
                          regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
                          regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('content_size'))
split_df.show(truncate=False)

#Data Cleaning
base_df.filter(base_df['value'].isNull()).count()

#check how clean the data is
bad_rows_df = split_df.filter(split_df['host'].isNull() |
                              split_df['timestamp'].isNull() |
                              split_df['path'].isNull() |
                              split_df['status'].isNull() |
                             split_df['content_size'].isNull())
bad_rows_df.count()

#Find which column is affected
from pyspark.sql.functions import col, sum

def count_null(col_name):
  return sum(col(col_name).isNull().cast('integer')).alias(col_name)

# Build up a list of column expressions, one per column.
#
# This could be done in one line with a Python list comprehension, but we're keeping
# it simple for those who don't know Python very well.
exprs = []
for col_name in split_df.columns:
  exprs.append(count_null(col_name))

# Run the aggregation. The *exprs converts the list of expressions into
# variable function arguments.
split_df.agg(*exprs).show()

#Let's see if there are any lines that do not end with one or more digits.
bad_content_size_df = base_df.filter(~ base_df['value'].rlike(r'\d+$'))
bad_content_size_df.count()

# Take a look at some of the bad column values. Since it's possible that the rows end in extra white space, we'll tack a marker character onto
# the end of each line, to make it easier to see trailing white space.

from pyspark.sql.functions import lit, concat
bad_content_size_df.select(concat(bad_content_size_df['value'], lit('*'))).show(truncate=False)

#Fix the rows with null content_size
# Replace all null content_size values with 0.
cleaned_df = split_df.na.fill({'content_size': 0})

# Ensure that there are no nulls left.
exprs = []
for col_name in cleaned_df.columns:
  exprs.append(count_null(col_name))

cleaned_df.agg(*exprs).show()

#Parsing the timestamp.

month_map = {
  'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
  'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12
}

def parse_clf_time(s):
    """ Convert Common Log time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    # NOTE: We're ignoring time zone here. In a production application, we want to handle that.
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
      int(s[7:11]),
      month_map[s[3:6]],
      int(s[0:2]),
      int(s[12:14]),
      int(s[15:17]),
      int(s[18:20])
    )

u_parse_time = udf(parse_clf_time)

logs_df = cleaned_df.select('*', u_parse_time(cleaned_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp')
total_log_entries = logs_df.count()
logs_df.printSchema()

display(logs_df)

#Cache logs_df. We're going to be using it quite a bit from here forward.
logs_df.cache()

#Content Size Statistics
#Calculate statistics based on the content size.
content_size_summary_df = logs_df.describe(['content_size'])
content_size_summary_df.show()

#We can use SQL to directly calculate these statistics
from pyspark.sql import functions as sqlFunctions
content_size_stats =  (logs_df
                       .agg(sqlFunctions.min(logs_df['content_size']),
                            sqlFunctions.avg(logs_df['content_size']),
                            sqlFunctions.max(logs_df['content_size']))
                       .first())

print 'Using SQL functions:'
print 'Content Size Avg: {1:,.2f}; Min: {0:.2f}; Max: {2:,.0f}'.format(*content_size_stats)

#HTTP Status Analysis
status_to_count_df =(logs_df
                     .groupBy('status')
                     .count()
                     .sort('status')
                     .cache())

status_to_count_length = status_to_count_df.count()
print 'Found %d response codes' % status_to_count_length
status_to_count_df.show()

display(status_to_count_df)
log_status_to_count_df = status_to_count_df.withColumn('log(count)', sqlFunctions.log(status_to_count_df['count']))

display(log_status_to_count_df)

# np is just an alias for numpy.
# cm and plt are aliases for matplotlib.cm (for "color map") and matplotlib.pyplot, respectively.
# prepareSubplot is a helper.
from spark_notebook_helpers import prepareSubplot, np, plt, cm
data = log_status_to_count_df.drop('count').collect()
x, y = zip(*data)
index = np.arange(len(x))
bar_width = 0.7
colorMap = 'Set1'
cmap = cm.get_cmap(colorMap)

fig, ax = prepareSubplot(np.arange(0, 6, 1), np.arange(0, 14, 2))
plt.bar(index, y, width=bar_width, color=cmap(0))
plt.xticks(index + bar_width/2.0, x)
display(fig)

#Frequent Hosts
# Any hosts that has accessed the server more than 10 times.
host_sum_df =(logs_df
              .groupBy('host')
              .count())

host_more_than_10_df = (host_sum_df
                        .filter(host_sum_df['count'] > 10)
                        .select(host_sum_df['host']))

print 'Any 20 hosts that have accessed more then 10 times:\n'
host_more_than_10_df.show(truncate=False)

#Visualize the number of hits to paths (URIs) in the log
paths_df = (logs_df
            .groupBy('path')
            .count()
            .sort('count', ascending=False))

paths_counts = (paths_df
                .select('path', 'count')
                .map(lambda r: (r[0], r[1]))
                .collect())

paths, counts = zip(*paths_counts)

colorMap = 'Accent'
cmap = cm.get_cmap(colorMap)
index = np.arange(1000)

fig, ax = prepareSubplot(np.arange(0, 1000, 100), np.arange(0, 70000, 10000))
plt.xlabel('Paths')
plt.ylabel('Number of Hits')
plt.plot(index, counts[:1000], color=cmap(0), linewidth=3)
plt.axhline(linewidth=2, color='#999999')
display(fig)
display(paths_df)

#Find the top paths (URIs) in the log.
# Top Paths
print 'Top Ten Paths:'
paths_df.show(n=10, truncate=False)

#Find Top Ten Error Paths
# DataFrame containing all accesses that did not return a code 200
from pyspark.sql.functions import desc
not200DF = logs_df.filter(logs_df.status != 200)
#print not200DF
not200DF.show(10)
# Sorted DataFrame containing all paths and the number of times they were accessed with non-200 return code
logs_sum_df = not200DF.groupBy('path').count().sort('count',ascending=False)

print 'Top Ten failed URLs:'
logs_sum_df.show(10, False)

#Find Number of Unique Hosts
unique_host_count = logs_df.select('host').distinct().count()
print 'Unique hosts: {0}'.format(unique_host_count)

#Number of Unique Daily Hosts
from pyspark.sql.functions import dayofmonth

day_to_host_pair_df = logs_df.select('host',dayofmonth('time').alias('day'))
#day_to_host_pair_df.show(10,False)
day_group_hosts_df = day_to_host_pair_df.dropDuplicates()
#day_group_hosts_df.show(10,False)
daily_hosts_df = day_group_hosts_df.groupBy('day').count()

print 'Unique hosts per day:'
daily_hosts_df.show(30, False)
daily_hosts_df.cache()

#Visualizing the Number of Unique Daily Hosts
days_with_hosts =[]
hosts = []
for i in range (len(daily_hosts_df.collect())):
    days_with_hosts.append(daily_hosts_df.collect()[i][0])
    hosts.append(daily_hosts_df.collect()[i][1])
#print(daily_hosts_df.collect())
#print(days_with_hosts)
#print(days_with_hosts[0][1])
print(hosts)

fig, ax = prepareSubplot(np.arange(0, 30, 5), np.arange(0, 5000, 1000))
colorMap = 'Dark2'
cmap = cm.get_cmap(colorMap)
plt.plot(days_with_hosts, hosts, color=cmap(0), linewidth=3)
plt.axis([0, max(days_with_hosts), 0, max(hosts)+500])
plt.xlabel('Day')
plt.ylabel('Hosts')
plt.axhline(linewidth=3, color='#999999')
plt.axvline(linewidth=2, color='#999999')
display(fig)

display(daily_hosts_df)

#Average Number of Daily Requests per Host
total_req_per_day_df = logs_df.groupBy(dayofmonth('time').alias('day')).count()
#total_req_per_day_df.show()
avg_daily_req_per_host_df = (
  (total_req_per_day_df.alias("total")
    .join(daily_hosts_df.alias("host"), ["day"])
    .select(col("day"), (col("total.count") / col("host.count")).alias("avg_reqs_per_host_per_day")))
)

print 'Average number of daily requests per Hosts is:\n'
avg_daily_req_per_host_df.show()
avg_daily_req_per_host_df.cache()

#Visualizing the Average Daily Requests per Unique Host
days_with_avg = (avg_daily_req_per_host_df.select('day').collect())
avgs = (avg_daily_req_per_host_df.select('avg_reqs_per_host_per_day').collect())
for i in range (len(avgs)):
  days_with_avg[i]=days_with_avg[i][0]
  avgs[i]=avgs[i][0]

print(days_with_avg)
print(avgs)

fig, ax = prepareSubplot(np.arange(0, 20, 5), np.arange(0, 16, 2))
colorMap = 'Set3'
cmap = cm.get_cmap(colorMap)
plt.plot(days_with_avg, avgs, color=cmap(0), linewidth=3)
plt.axis([0, max(days_with_avg), 0, max(avgs)+2])
plt.xlabel('Day')
plt.ylabel('Average')
plt.axhline(linewidth=3, color='#999999')
plt.axvline(linewidth=2, color='#999999')
display(fig)

display(avg_daily_req_per_host_df)

#Exploring 404 Status Codes
#Counting 404 Response Codes
not_found_df = logs_df.filter(logs_df.status==404)
print('Found {0} 404 URLs').format(not_found_df.count())
not_found_df.cache()

#Listing 404 Status Code Records
not_found_paths_df = not_found_df.select('path')
unique_not_found_paths_df = not_found_paths_df.distinct()

print '404 URLS:\n'
unique_not_found_paths_df.show(n=40, truncate=False)

#Listing the Top Twenty 404 Response Code paths
top_20_not_found_df = not_found_paths_df.groupBy('path').count().sort('count',ascending=False)

print 'Top Twenty 404 URLs:\n'
top_20_not_found_df.show(n=20, truncate=False)

#Listing the Top Twenty-five 404 Response Code Hosts
hosts_404_count_df = not_found_df.groupBy('host').count().sort('count',ascending=False)

print 'Top 25 hosts that generated errors:\n'
hosts_404_count_df.show(n=25, truncate=False)

#Listing 404 Errors per Day
errors_by_date_sorted_df = not_found_df.groupBy(dayofmonth('time').alias('day')).count()

print '404 Errors by day:\n'
errors_by_date_sorted_df.show()
errors_by_date_sorted_df.cache()

#Visualizing the 404 Errors by Day
days_with_errors_404 = errors_by_date_sorted_df.select('day').collect()
#print days_with_errors_404[2][0]
errors_404_by_day = errors_by_date_sorted_df.select('count').collect()
for i in range (len(days_with_errors_404)):
  days_with_errors_404[i]=days_with_errors_404[i][0]
  errors_404_by_day[i]=errors_404_by_day[i][0]

print days_with_errors_404
print errors_404_by_day

fig, ax = prepareSubplot(np.arange(0, 20, 5), np.arange(0, 600, 100))
colorMap = 'rainbow'
cmap = cm.get_cmap(colorMap)
plt.plot(days_with_errors_404, errors_404_by_day, color=cmap(0), linewidth=3)
plt.axis([0, max(days_with_errors_404), 0, max(errors_404_by_day)])
plt.xlabel('Day')
plt.ylabel('404 Errors')
plt.axhline(linewidth=3, color='#999999')
plt.axvline(linewidth=2, color='#999999')
display(fig)

display(errors_by_date_sorted_df)

#Top Five Days for 404 Errors
top_err_date_df = errors_by_date_sorted_df.sort('count',ascending=False)

print 'Top Five Dates for 404 Requests:\n'
top_err_date_df.show(5)

#Hourly 404 Errors
from pyspark.sql.functions import hour
hour_records_sorted_df = not_found_df.groupBy(hour('time').alias('hour')).count()

print 'Top hours for 404 requests:\n'
hour_records_sorted_df.show(24)
hour_records_sorted_df.cache()

#Visualizing the 404 Response Codes by Hour
hours_with_not_found =map (lambda hour: hour[0],(hour_records_sorted_df.select('hour').collect()))
not_found_counts_per_hour = map (lambda counts: counts[0],(hour_records_sorted_df.select('count').collect()))

print hours_with_not_found
print not_found_counts_per_hour

fig, ax = prepareSubplot(np.arange(0, 25, 5), np.arange(0, 500, 50))
colorMap = 'seismic'
cmap = cm.get_cmap(colorMap)
plt.plot(hours_with_not_found, not_found_counts_per_hour, color=cmap(0), linewidth=3)
plt.axis([0, max(hours_with_not_found), 0, max(not_found_counts_per_hour)])
plt.xlabel('Hour')
plt.ylabel('404 Errors')
plt.axhline(linewidth=3, color='#999999')
plt.axvline(linewidth=2, color='#999999')
display(fig)

display(hour_records_sorted_df)





























































































































































































































































