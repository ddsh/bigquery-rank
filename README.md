## BigQuery Rank

#### Principle
Computes a new table in bigquery with the rank with respect to  
a specified field.  
    1. Extract the bigquery src_table to storage in a specified bucket  
    2. Download the files in /tmp  
    3. Uncompress files - sort - rank  
    4. Upload ranked file to bigquery  
    
#### Example:  

Input table:

| id        |  historical_num_purchases   |  
| --------- | --------------------------- |  
| 'Alice'   |             4               |  
| 'Bob'     |             3               |  
| 'Charlie' |             5               |  

Output table:

| id        |  historical_num_purchases   | rank |
| --------- | --------------------------- | ---- |  
| 'Alice'   |             4               |   2  |
| 'Bob'     |             3               |   3  |
| 'Charlie' |             5               |   1  |


### Arguments
 project : Google Cloud Platform Project Id  
 bucket :  Storage Bucket Id  
 dataset :  BigQuery Dataset Id  
 src_table : BigQuery Source Table  
 dst_table : BigQuery Destination Table  
 field : Field of the source table for sorting  
 reverse : Reverse Sorting  
 numerical : Numerical Sorting  


### Usage
~~~~
  bigquery_rank.py <project> <bucket> <dataset> <src_table> <dst_table> <field> [--reverse] [--numerical]
  bigquery_rank.py -h | --help
~~~~
