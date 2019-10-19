# Bible Analytics with Pyspark

In this project, the King James Version (KJV) Bible was analyzed with Pyspark and Jupyter Notebook.

In rdd_kjv.ipynb notebook, Pyspark RDD was created for the KJV text file. Major RDD operations include:

(1) filter out the empty lines;
(2) break the text to separate lines and words with methods such as split(), map(), flatMap();
(3) filter out the extra words in the Bible(the title of each verse) with a regex object;
(4) remove the punctuations at the end or the beginning of some words so that the same words are not treated as different words due to association with a punctuation mark (regex objects were complied for this purpose);
(5) perform MapReduce operations and count the occurances of words for different scenarios: no case conversion, converting all words to lower case, converting all word to lower case except for a few special words such as 'God', 'Lord', 'Spirit'.

In spark_dataframe_sql_kjv.ipynb notebook, RDD was converted to DataFrame. Major operations on the dataframe include:

(1) index was generated for ease of operation. While in RDD, index was added using zipWithIndex() method after finding that the monotonically_increasing_id() method for dataframe could not guarantee the consecutiveness of row id's;
(2) columns of chapter names and book names were generated from the verse name strings using UDF's and added with withColumn() method;
(3) lists of chapter ID's and book ID's were generated using custom defined function, RDDs were created for the lists with index added, and were then converted to dataframes;
(4) columns of chapter ID's and book ID's were added to the dataframe of the Bible by joining the dataframes on the common indices;
(5) Spark SQL TempView was created for sql queries;
(6) SQL queries were also performed on the dataframe with dataframe functions.
