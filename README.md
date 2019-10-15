# Data Lake Compaction

Given an ADLS file system in which we have different partitions (e.g. *~/year/month/day/*), the **aim of this project** is to compact (format: snappy) the last partition's parquet files up to a given file size.

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

    <pre><code class="language-scala">
    // Hadoop
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net", "<your-data-lake-key>")
    spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", "abfss://<your-file-system-name>@<your-storage-account-name>.dfs.core.windows.net")
    // Spark
    spark.conf.set("fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net", "<your-data-lake-key>")
    </code></pre>
    
### Configuration file extraction
We will have a configuration file stored at our ADLS to which we well access using

    <pre><code class="language-scala">
    val configFile = spark.read.textFile(<configuration-file-path>).collect()
    </code></pre>
    
