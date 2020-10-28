# DataEngineerChallenge

## Overview

This document describes the solution to https://github.com/Pay-Baymax/DataEngineerChallenge.

The repo is only showing the analytical observations about the data using the distributed tool - Spark (Python) .

## Solution 

### Processing & Analytical goals: 
    
> All answer in `ans/data_engg_task.ipynb` file.
> Session window time: 15 mins.

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

    > ip_session_info is cancat of `client_ip` & `session_count`

    Result looks like this:
    ```
    +-----------------+-------------+
    |  ip_session_info|count_session|
    +-----------------+-------------+
    | 220.226.206.7_12|           12|
    | 220.226.206.7_11|           11|
    | 220.226.206.7_10|           10|
    | 54.255.254.236_9|            9|
    | 177.71.207.172_9|            9|
    |  54.241.32.108_9|            9|
    |  54.243.31.236_9|            9|
    |   54.232.40.76_9|            9|
    | 176.34.159.236_9|            9|
    | 54.252.254.204_9|            9|
    |  119.81.61.166_9|            9|
    |  54.240.196.33_9|            9|
    |   54.228.16.12_9|            9|
    |  54.252.79.172_9|            9|
    |  54.245.168.44_9|            9|
    | 120.29.232.107_9|            9|
    |168.235.197.238_9|            9|
    |  107.23.255.12_9|            9|
    |  106.186.23.95_9|            9|
    |   185.20.4.220_9|            9|
    +-----------------+-------------+
    only showing top 20 rows
    ```
2. Determine the average session time:
    
    > Each Session duration calculated from `current_timestamp - previous_timestamp`.
    
    Result looks like this:
    ```
    +------------------+
    |  avg_session_time|
    +------------------+
    |125.39079162978608|
    +------------------+
    ```

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    
    > Aggregrate the `request_url` of each `client_ip`   
    
    Result looks like this:
    ```
    +-----------------+---------------------+
    |  ip_session_info|count_unique_requests|
    +-----------------+---------------------+
    |205.175.226.101_0|                   89|
    |115.248.233.203_2|                   86|
    |  106.51.235.51_0|                   89|
    |  192.193.164.9_1|                   55|
    |   202.91.134.7_4|                   10|
    | 115.111.50.254_1|                   18|
    |  182.68.136.65_0|                  104|
    |    1.39.61.253_0|                   59|
    | 59.184.184.157_0|                    9|
    |  101.57.193.44_0|                   82|
    |115.242.129.233_0|                    7|
    | 223.255.247.66_0|                    7|
    |117.239.224.160_1|                   64|
    | 59.165.251.191_2|                   86|
    |  188.40.94.195_1|                   89|
    |    8.37.228.47_1|                   55|
    | 117.210.14.119_0|                    3|
    |   182.69.48.36_0|                  108|
    | 115.249.21.130_0|                   10|
    | 122.164.34.125_0|                    8|
    +-----------------+---------------------+
    only showing top 20 rows
    ```

4. Find the most engaged users, ie the IPs with the longest session times
    
    > Sum of all session duration of the IP (each session is 15 mins)

    Result looks like this:
    ```
    +---------------+----------------+------------+--------------------+----------------+
    |      client_ip|session_time_all|num_sessions|session_duration_max|avg_session_time|
    +---------------+----------------+------------+--------------------+----------------+
    |   27.120.106.3|   66298.9140625|           2|           66298.914|  33149.45703125|
    |117.255.253.155|     57422.78125|           2|            57422.78|    28711.390625|
    |     1.38.21.92|  54168.50390625|           2|           54168.504| 27084.251953125|
    | 163.53.203.235|   54068.0390625|           2|            54068.04|  27034.01953125|
    |   66.249.71.10|   53818.8046875|           2|           53818.805|  26909.40234375|
    |    1.38.22.103|  50599.78515625|           2|           50599.785| 25299.892578125|
    | 167.114.100.25|  50401.54296875|           2|           50401.543| 25200.771484375|
    |    75.98.9.249|   49283.2578125|           2|           49283.258|  24641.62890625|
    |107.167.112.248|      49079.5625|           2|           49079.562|     24539.78125|
    | 168.235.200.74|   48446.5703125|           2|            48446.57|  24223.28515625|
    | 122.174.94.202|     48349.15625|           2|           48349.156|    24174.578125|
    | 117.253.108.44|   46465.7421875|           2|           46465.742|  23232.87109375|
    | 117.244.25.135|  46324.63671875|           2|           46324.637| 23162.318359375|
    |  182.75.33.150|  46251.28515625|           2|           46251.285| 23125.642578125|
    |    8.37.225.38|   46245.2734375|           2|           46245.273|  23122.63671875|
    |      1.38.13.1|    46214.453125|           2|           46214.453|   23107.2265625|
    |    1.39.61.171|   46112.8359375|           2|           46112.836|  23056.41796875|
    |  49.156.86.219|  45112.31640625|           2|           45112.316| 22556.158203125|
    | 199.190.46.117|  45029.84765625|           2|           45029.848| 22514.923828125|
    |   122.15.56.59|  44929.15234375|           2|           44929.152| 22464.576171875|
    +---------------+----------------+------------+--------------------+----------------+
    only showing top 20 rows
    ```

## Additional questions for Machine Learning Engineer (MLE) candidates:
1. Predict the expected load (requests/second) in the next minute

> Details in `ans/ml_task_1.ipynb` file.

> Approach: time-series forcasting using fbProphet

```
+-------------------+----+
|               time|load|
+-------------------+----+
|2015-07-22 11:40:06|  27|
|2015-07-22 11:40:07|  62|
|2015-07-22 11:40:08|  56|
|2015-07-22 11:40:09| 112|
|2015-07-22 11:40:10|  58|
...
|2015-07-22 11:40:25| 130|
+-------------------+----+
only showing top 20 rows
```

2. Predict the session length for a given IP

> Details in `ans/ml_task_2.ipynb` file.

> Approach: devided IP in octet part and calculate the session length and trained data on `xgboost` and  `RandomForestRegressor` ensemble learning method
```
+------------------+------+------+------+------+
|    session_length|octet0|octet1|octet2|octet3|
+------------------+------+------+------+------+
| 69.81707191467285|     1|   186|    41|     1|
| 231.7906957184896|     1|   186|    76|    11|
| 33.04862296581268|     1|   187|   228|   210|
| 33.92300724051893|     1|   187|   228|    88|
| 59.14387809485197|     1|    23|   101|   102|
| 9.247098922729492|     1|    23|   226|    88|
| 210.9620418548584|     1|    38|    21|    65|
...
| 56.01950880885124|     1|    39|    62|   195|
+------------------+------+------+------+------+
only showing top 20 rows
```

3. Predict the number of unique URL visits by a given IP

> Details in `ans/ml_task_3.ipynb` file.

> Approach: devided IP in octet part and calculate unique url and trained data on `xgboost` and  `RandomForestRegressor` ensemble learning

```
+-----------------+------+------+------+------+
|count_unique_URLs|octet0|octet1|octet2|octet3|
+-----------------+------+------+------+------+
|               14|    59|   160|   110|   163|
|                6|    27|    63|   186|    72|
|                7|   120|    61|    47|    36|
|               85|   115|   112|   250|   108|
|               16|    61|    16|   142|   162|
|               10|   123|   136|   182|   137|
|               16|   117|   205|    39|   248|
...
|                7|   123|    63|   194|   202|
+-----------------+------+------+------+------+
only showing top 20 rows
```
