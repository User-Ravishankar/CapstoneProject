Problem Statement: The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.



The application should have the following features:

1.Take input as a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.

2.Option to store the data in a MongoDB database as a data lake and collect data from different YouTube channels and store them in the data lake then select a channel name and migrate its data from the data lake to a SQL database as tables.

3.Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

4.YouTube API: You'll need to use the YouTube API to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.

5.Store data in a MongoDB data lake: Once you retrieve the data from the YouTube API, you can store it in a MongoDB data lake. 

6.Migrate data to a SQL data warehouse: After you've collected data for multiple channels, you can migrate it to a SQL data warehouse.

7.Query the SQL data warehouse: You can use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. You can use a Python SQL library such as SQLAlchemy to interact with the SQL database.

8. Display data in the Streamlit app: Finally, you can display the retrieved data in the Streamlit app. Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.
