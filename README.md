# openWeather_taskflow

The python scipt extract function uses asyncronous programming to run http requests concurrently.
The transform function creates seperate dataframes from the json objects to load into corresponding tables in a data warehouse.
Warehouse uses star schema with auto incrementing primary keys for the dimension tables and UUID for dim tables primary keys with trigger to ensure uniqueness. In such a situation its best practice to load the data into the dimension tables before the fact table, otherwise errors relating to the constraints of the fact tables foreign keys will be triggered. So the load functions push to dim tables then query them for their keys and add to the load function for the fact table.


![Screenshot 2024-08-09 at 2 17 53 PM](https://github.com/user-attachments/assets/63c0f7ed-a3d8-4d68-a4bf-ef1cf4129f54)
