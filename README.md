# Retail_Data_Analysis
## Problem Statement
The online market has grown tremendously in the past few years. Digitally enabled customers like to shop on the run, and that is the reason why online shopping is one of the most popular online activities worldwide. In 2019, global e-commerce sales amounted to 3.53 trillion USD and are projected to grow to 6.54 trillion USD in 2022.

The analytical capabilities of big data have had a positive impact across industries, including e-commerce. Big data tools improve business performance by enabling companies to analyse trends and current consumer behavioural patterns and offer better and more customised products.

For the purposes of this project, you have been tasked with computing various Key Performance Indicators (KPIs) for an e-commerce company, RetailCorp Inc. You have been provided real-time sales data of the company across the globe. The data contains information related to the invoices of orders placed by customers all around the world. You will get to know the details of the data in the next segment.

At the industry level, an end-to-end data pipeline is built for this purpose. Tools such as HDFS(Hadoop Distributed File System) are used to store the data that is processed by the real-time processing framework and then shown on a dashboard with tools such as Tableau and PowerBI. The image given below is an example of such a complete data pipeline.

### Broadly, we will perform the following tasks in this project:

- Reading the sales data from the Kafka server
- Preprocessing the data to calculate additional derived columns such as total_cost etc
- Calculating the time-based KPIs and time and country-based KPIs
- Storing the KPIs (both time-based and time- and country-based) for a 10-minute interval into separate JSON files for further analysis
### As you can see, the data contains the following information:

- Invoice number: Identifier of the invoice
- Country: Country where the order is placed
- Timestamp: Time at which the order is placed
- Type: Whether this is a new order or a return order
- SKU (Stock Keeping Unit): Identifier of the product being ordered
- Title: Name of the product is ordered
- Unit price: Price of a single unit of the product
- Quantity: Quantity of the product being ordered

Reading input data from Kafka Code to take raw Stream data from Kafka server Details of the Kafka broker are as follows: Bootstrap Server - 18.211.252.152 Port - 9092 Topic - real-time-project Calculating additional columns and writing the summarised input table to the console

The following attributes from the raw Stream data have to be taken into account for the project: invoice_no: Identifier of the invoice country: Country where the order is placed timestamp: Time at which the order is placed

### In addition to these attributes, the following UDFs have to be calculated and added to the table:

- total_cost: Total cost of an order arrived at by summing up the cost of all products in that invoice (The return cost is treated as a loss. Thus, for return orders, this value will be negative.)

- total_items: Total number of items present in an order

- is_order: This flag denotes whether an order is a new order or not. If this invoice is for a return order, the value should be 0.

- is_return: This flag denotes whether an order is a return order or not. If this invoice is for a new sales order, the value should be 0.

- The input table must be generated for each one-minute window.

- Code to define the schema of a single order

- Code to define the aforementioned UDFs and any utility functions are written to calculate them

- Code to write the final summarised input values to the console. This summarised input table has to be stored in a Console-output file. This can be done by simply appending ‘> file-name’ to the Spark-Submit code as follows: spark2-submit_command > file_name

### Calculating time-based KPIs:

- Code to calculate time-based KPIs tumbling window of one minute on orders across the globe. These KPIs were discussed in the previous segment.
- KPIs have to be calculated for a 10-minute interval for evaluation; so, ten 1-minute window batches have to be taken.
- Time-based KPIs can be structured like below. (These tables do not need to be outputted and are just for reference as to how your KPI tables must be structured when all the files are combined.)
### Calculating time- and country-based KPIs:

- Code to calculate time- and country-based KPIs tumbling window of one minute on orders on a per-country basis. These KPIs were discussed in the previous segment.
- KPIs have to be calculated for a 10-minute interval for evaluation; so, ten 1-minute window batches have to be taken.
- Time- and country-based KPIs can be structured like below. (These tables do not need to be outputted and are just for reference as to how your KPI tables must be structured.)
### Writing all the KPIs to JSON files:

- Code to write the KPIs calculated above into JSON files for each one-minute window.
- These have to be written for a 10-minute interval.

