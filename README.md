# Data lake files compaction

Given an ADLS file system in which we have different partitions (e.g. *~/year/month/day/*), the **aim of this project** is to compact (format: snappy) **the last partition's parquet files** up to a given file size.

## Preliminaries
### Project configuration
|JDK | SDK|
|:--:| :--:|
| 1.8| 2.11.12|

### Run configuration
| VMOptions | Program arguments |
|:--:| :--:|
| -Xmx512m| -p *your-configuration-file-name*.conf|

The configuration file name must be at the directory **abfss://FILE-SYSTEM-NAME@STORAGE-ACCOUNT-NAME.dfs.windows.core.net/**
On the contrary, you must specify the directory path not including the one written above. For example:

    -p my-full-path/my-configuration-file.conf
    
This project is using credentials from Azure Data Lake Storage (ADLS). In order to get a successful compilation, you should change the the ADLS file system name, its storage account name and the ADLS access key (you can also use a key-vault method) in the code where they are required.

### IDE
IntelliJ is highly recommended.

## How does it work?
For a better comprehension, in this section we will explain the steps followed during the code development.

### ADLS connection
Firstly, we must establish connection with ADLS. We are using Spark and also Hadoop inside a Spark Context, so we need to set the ADLS credentials onto them:

    // Hadoop
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net", "<your-data-lake-key>")
    spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", "abfss://<your-file-system-name>@<your-storage-account-name>.dfs.core.windows.net")
    // Spark
    spark.conf.set("fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net", "<your-data-lake-key>")

### Configuration file extraction
We will have a configuration file stored at our ADLS to which we well access using

    val configFile = spark.read.textFile(<configuration-file-path>).collect()
    
In the following box we will specify the configuration file format:
    size=<compacted-files-size>
    origin_path="abfss://<your-file-system-name>@<your-storage-account-name>.dfs.core.windows.net/<your-data-path>"
    partitions=["partition1","partition2","partitionN"]
    filter=[["partition1"],["partition1","partition2"],["partition1","partition2","partition3"]]
    
The filter attribute contains an example of different types of filter that can be applied when compacting. It will always be a list of lists. In order to compact everything from *origin_path*, just let filter be [[]].

It is also important that **size** must be specified in **MB**.

### Arguments parsing
The configuration file is parsed and we generate a path list to compact taking into account *filter* attribute.
    
### Compaction

#### Partition size
In this part we create a Hadoop File System in order to calculate the partition size.

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val size = fs.getContentSummary(new Path(<path-from-path-list>)).getLength

#### Number of resulting files after compaction
    
    val fileNum = size/(<size-from-configuration-file> * 1024 * 1024) + 1 // MB to B

#### Compaction at temporary directory
We take every parquet file at the directory and we create a partition of the data frame using *fileNum* and write it on a temporary directory.

    val df = spark.read.parquet(<path-from-path-list> + "/")
    df.repartition(fileNum.toInt).write.mode(SaveMode.Append).parquet(<pathfrom-path-list> + "-tmp")
    
When the compaction has been successfully accomplished, a new file called *_SUCCESS* with the commit information is created.

#### Data dump and deletion of temporary directory

At the end of the process, we loop over the paths list that has been generated from the filters setted by the user and, for each one, we do:

    val p = new Path(<path-from-path-list>)
    val pTmp = new Path(<path-from-path-list> + "-tmp")
    fs.mkdirs(<path-from-path-list> + "-tmp")
    depthCompaction(<path-from-path-list> + "/", config.size)
    fs.delete(p, true)
    fs.rename(p, pTmp)
