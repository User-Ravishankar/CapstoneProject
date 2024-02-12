import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector
import pymongo
from googleapiclient.discovery import build


# Creating a connection with MongoDB and Creating a new database
client = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = client["youtube_data"]

# Connecting with SQl Database
mydb_sql = mysql.connector.connect(
                                  host="localhost",
                                  user="root",
                                  password="P2s4word9@@",
                                  auth_plugin='mysql_native_password',
                                  database='youtube'
                                )
mycursor = mydb_sql.cursor(buffered=True)

#Creating a connection with Youtube API
api_key='AIzaSyDSC74hcLbENBLSGTRmGhr5Dbh5J9gcJq8'
youtube = build('youtube', 'v3', developerKey=api_key)


# Function to Fetch Channel Details
def get_channel_details(channel_id):
    channel_data = []
    response = youtube.channels().list(part='snippet,contentDetails,statistics',
                                       id=channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_Id=channel_id[i],
                    Channel_name=response['items'][i]['snippet']['title'],
                    Playlist_Id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscription_Count=response['items'][i]['statistics']['subscriberCount'],
                    Channel_Views=response['items'][i]['statistics']['viewCount'],
                    Channel_Description=response['items'][i]['snippet']['description'],
                    )
        channel_data.append(data)
    return channel_data



# Function to Fetch Video ids
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    response = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        response = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids



# Function to Fetch Video Details

def get_video_details(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
                                part="snippet,ContentDetails,statistics",
                                id=video_id)
        response = request.execute()

        for video in response['items']:
            video_details = dict(Channel_name=video['snippet']['channelTitle'],
                                 Channel_id=video['snippet']['channelId'],
                                 Video_id=video['id'],
                                 Title=video['snippet']['title'],
                                 Tags=video['snippet'].get('tags'),
                                 Thumbnail=video['snippet']['thumbnails']['default']['url'],
                                 Description=video['snippet']['description'],
                                 Published_date=video['snippet']['publishedAt'],
                                 Duration=video['contentDetails']['duration'],
                                 Views=video['statistics']['viewCount'],
                                 Likes=video['statistics'].get('likeCount'),
                                 Comments=video['statistics'].get('commentCount'),
                                 Favorite_count=video['statistics']['favoriteCount'],
                                 Definition=video['contentDetails']['definition'],
                                 Caption_status=video['contentDetails']['caption']
                                 )
            video_data.append(video_details)
    return video_data


#Function to Fetching Comment details

def get_comment_details(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                                part='snippet',
                                videoId=video_id,
                                maxResults=50)
            response = request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                          Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Comment_PublishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt'])

                comment_data.append(data)
    except:
        pass
    return comment_data

# Function to Fetch Channel names from MongoDB
def channel_names():
    channel_name = []
    for i in mydb.channel_details.find():
        channel_name.append(i['Channel_name'])
    return channel_name

# Home Page
# Setting up sidebar in streamlit page with required options
with st.sidebar:
    selected = option_menu("Main Menu",
                           ['Application Details', "View Details & Upload to MongoDB", "Migrate to MySQL Database","Transform Data","SQL_View"],
                           icons=['app', 'cloud-upload', "database", "$\huge"],
                           menu_icon="menu-up",
                           orientation="vertical")
# Setting up the option 'Application Details' in streamlit page
if selected == 'Application Details':
    st.title(":red[YouTube Data Harvesting & Warehousing]")


# Setting up option 'View Details & Upload to MongoDB' in streamlit page
if selected == 'View Details & Upload to MongoDB':
    st.markdown("Enter the YouTube channel ID ")
    channel_id = st.text_input("channel ID")

    if channel_id and st.button("Extract Data"):
        ch_details = get_channel_details(channel_id)
        st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
        st.table(ch_details)

    if st.button("Upload to MongoDB"):
        with st.spinner('Please Wait for it...'):
            ch_details = get_channel_details(channel_id)
            video_ids = get_channel_videos(channel_id)
            vid_details = get_video_details(video_ids)


            def comments():
                comment_data = []
                for i in video_ids:
                    comment_data += get_comment_details(i)
                return comment_data


            comm_details = comments()

            collections1 = mydb.channel_details
            collections1.insert_many(ch_details)

            collections2 = mydb.video_details
            collections2.insert_many(vid_details)

            collections3 = mydb.comments_details
            collections3.insert_many(comm_details)
            st.success("Upload to MongoDB successful !!")

            try:
                extracted_details = get_channel_details(channel_id)
            except KeyError:
                st.error("Invalid channelID.Enter valid input ID")


    if selected == "Transform Data":
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel", options=ch_names)


def insert_into_channels():
    collections = mydb.channel_details
    query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s)"""

    for i in collections.find({"channel_name": user_inp}, {'_id': 0}):
        mycursor.execute(query, tuple(i.values()))
    mydb_sql.commit()


def insert_into_videos():
    collections1 = mydb.video_details
    query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    for i in collections1.find({"channel_name": user_inp}, {'_id': 0}):
        values = [str(val).replace("'", "''").replace('"', '""') if isinstance(val, str) else val for val in
                  i.values()]
        mycursor.execute(query1, tuple(values))
        mydb_sql.commit()


def insert_into_comments():
    collections1 = mydb.video_details
    collections2 = mydb.comments_details
    query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s)"""

    for vid in collections1.find({"channel_name": user_inp}, {'_id': 0}):
        for i in collections2.find({'Video_id': vid['Video_id']}, {'_id': 0}):
            mycursor.execute(query2, tuple(i.values()))
            mydb_sql.commit()


if st.button("Submit"):
    try:
        insert_into_videos()
        insert_into_channels()
        insert_into_comments()
        st.success("Transformation to MySQL Successful !!")
    except:
        st.error("Channel details already transformed !!")


if selected == "SQL_View":
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
                             ['1. What are the names of all the videos and their corresponding channels?',
                              '2. Which channels have the most number of videos, and how many videos do they have?',
                              '3. What are the top 10 most viewed videos and their respective channels?',
                              '4. How many comments were made on each video, and what are their corresponding video names?',
                              '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                              '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                              '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                              '8. What are the names of all the channels that have published videos in the year 2022?',
                              '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                              '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT Video_name AS Video_name, channel_name AS Channel_Name
                                FROM videos
                                ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                                FROM channels
                                ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        # st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, Video_name AS Video_Title, View_count AS Views 
                                FROM videos
                                ORDER BY views DESC
                                LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, Video_name AS Video_Title, b.Total_Comments
                                FROM videos AS a
                                LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                                FROM comments GROUP BY video_id) AS b
                                ON a.video_id = b.video_id
                                ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,Video_name AS Title,Like_count AS Like_Count 
                                FROM videos
                                ORDER BY Like_count DESC
                                LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT Video_name AS Title, Like_count AS Like_count
                                FROM videos
                                ORDER BY Like_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, channel_views AS Views
                                FROM channels
                                ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name
                                FROM videos
                                WHERE published_date LIKE '2022%'
                                GROUP BY channel_name
                                ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,
                                AVG(duration)/60 AS "Average_Video_Duration (mins)"
                                FROM videos
                                GROUP BY channel_name
                                ORDER BY AVG(duration)/60 DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,Video_id AS Video_ID,Comment_count AS Comments
                                FROM videos
                                ORDER BY comments DESC
                                LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)
