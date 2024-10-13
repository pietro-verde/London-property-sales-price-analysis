# London Property Sales Price Analysis

Welcome to this repository! Here, you'll find an in-depth analysis of property sales prices in London. This project aims to provide insights into the real estate market trends in one of the world's most vibrant cities.

## Project Overview

This project is divided into several parts:

1. **Data Extraction**: The data has been collected data from the [HM Land Registry Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads) and from the [Energy Performance Certificates (EPCs)](https://epc.opendatacommunities.org/).

2. **Data Wrangling and Transformation**: The raw data is processed using Python in a Jupyter notebook. With the help of Spark, pandas, and matplotlib, the data has been cleaned, transformed, and structured to make it suitable for analysis and visualization.

3. **Dataset Creation**: The cleaned and transformed data is compiled into a dataset. This dataset serves as the foundation for the visualizations and further analysis. The obtained dataset is not listed in this repository, but can be generated using the steps outlined in the Jupyter notebook.

4. **Visualization**: The dataset is used to create an interactive visualization hosted on Tableau. This visualization allows users to explore the trends in London's property sales prices.

You'll find the Tableau dashboard here: https://public.tableau.com/app/profile/pietro.verde/viz/LondonPropertySalesPrice/PropertySales

Feel free to explore the repository, run the Jupyter notebook, and interact with the Tableau visualization. I hope this project provides valuable insights and serves as a useful resource for understanding London's property market.

Happy exploring! 🏠📊




*Contains HM Land Registry data and Energy Performance Certificates data © Crown copyright and database right 2021. This data is licensed under the Open Government Licence v3.0.*



------- to add -------------

#### Load EPC data

The UK Price Paid Data provides valuable information about property transactions. However, it's important to note that this dataset does not include the size of the properties, which can be useful to calculate price per square meter. 

To obtain property size data, we can query the Energy Performance of Buildings Data (EPC data). The EPC data contains detailed information about the energy performance of properties, including their size. 

By combining these two datasets, we can gain a more comprehensive understanding of the property market in the UK.

The EPC data was downloaded from https://epc.opendatacommunities.org/domestic/search. Registration required.

Since the EPC data is available in files divided by district, beofore proceeding with processing the data, it is necessary to reference a list of all London districs names and codes, which can be obtained from the [Office of National Statistics website](https://geoportal.statistics.gov.uk/datasets/ons::local-authority-districts-april-2023-names-and-codes-in-the-united-kingdom/explore) , together with a list of London boroughs that is available from Wikipedia. 