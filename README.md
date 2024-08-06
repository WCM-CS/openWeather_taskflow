# openWeather_taskflow

The python scipt extract function uses asyncronous programming to run http requests concurrently
The transform function creates seperate dataframes from the json objects to load into corresponding tables in a data warehouse.
Warehouse uses star schema with auto incrementing primary keys for the dimension tables. In such a situation its best practice to load the data into the dimension tables before the fact table, otherwise errors relating to the constraints of the fact tables foreign keys will be triggered. 

<img width="755" alt="Screenshot 2024-08-05 at 3 32 24 AM" src="https://github.com/user-attachments/assets/79ed8443-f5e3-410c-8ac3-fc70f841ea46">
