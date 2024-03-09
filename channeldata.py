import googleapiclient.discovery
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(
api_service_name, api_version,developerKey="AIzaSyCia8jjKSJKYOyBNOXbfBjFbrOhlp56qh8")
# GETTING THE CHANNEL DATA
def channel_data(cid):
  request = youtube.channels().list(
          part="snippet,contentDetails,statistics",
          id=cid
      )
  response = request.execute()
  channel_data={
      'channel_name':response['items'][0]['snippet']['title'],
      'channel_id':response['items'][0]['id'],
      'subscriber_count':response['items'][0]['statistics']['subscriberCount'],
      'view_count':response['items'][0]['statistics']['viewCount'],
      'description':response['items'][0]['snippet']['localized']['description'],
      'playlist_id':response['items'][0]['contentDetails']['relatedPlaylists']['uploads']}
  return channel_data
# GETTING VIDEO ID FROM PLAYLIST DATA
def playlist_data(cid):
  request = youtube.channels().list(
            part="snippet,contentDetails",
            id=cid)

  response = request.execute()
  next_page_token = None
  playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  while True:
    request = youtube.playlistItems().list(
                    part='snippet,contentDetails',
                    playlistId = playlist_id,
                    maxResults =50,
                    pageToken=next_page_token)
    response = request.execute()
    playlist_data=[]
    for i in range(len(response['items'])):
      playlist_data1={'playlist_id':response['items'][i]['contentDetails']['videoId'],
                    'channel_id':cid,
                    'playlist_name':response['items'][0]['snippet']['title']}
      playlist_data.append(playlist_data1)
    next_page_token=response.get("nextPageToken")
    if next_page_token is None:
      break
  return playlist_data
#GETTING ALL VIDEO DATAS OF A CHANNEL
def video_data(cid):
  playlist_detail=playlist_data(cid)
  video_idd=[]
  for i in playlist_detail:
    video_idd.append(i['playlist_id'])
  video_data=[]
  j=0
  for j in video_idd:
    request = youtube.videos().list(
                  part="snippet,contentDetails,statistics",
                  id=j
          )
    response = request.execute()
    video_data1={'video_id':j,
                      'video_name':response['items'][0]['snippet']['title'],
                      'channel_id':cid,
                      'video_description':response['items'][0]['snippet']['description'],
                      'tags':[response['items'][0]['etag'],response['etag']],
                      'published_at':response['items'][0]['snippet']['publishedAt'],
                      'view_count':response['items'][0]['statistics']['viewCount'],
                      'like_count':response['items'][0]['statistics']['likeCount'],
                      'favorite_count':response['items'][0]['statistics']['favoriteCount'],
                      'comment_count':response['items'][0]['statistics']['commentCount'],
                      'duration':response['items'][0]['contentDetails']['duration'],
                      'thumbnails':response['items'][0]['snippet']['thumbnails']['default']['url'],
                      'caption_status':response['items'][0]['contentDetails']['caption']}
    video_data.append(video_data1)
    j+=j
  return video_data
# GETTING ALL COMMENT DETAILS OF EACH VIDEO
def comments_data(cid):
  playlist_detail=playlist_data(cid)
  video_idd=[]
  for i in playlist_detail:
    video_idd.append(i['playlist_id'])
  comment_data=[]
  try:
    for k in video_idd:
      request = youtube.commentThreads().list(
              part="snippet",
              videoId=k,
              maxResults=50
                      )
      response = request.execute()
      if len(response['items'])>0:
        comment_data1={'channel_id':cid,
                       'video_id':k,
                       'comment_id':response['items'][0]['snippet']['topLevelComment']['id'],
                       'comment_text':response['items'][0]['snippet']['topLevelComment']['snippet']['textDisplay'],
                       'comment_author':response['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                       'comment_published_at':response['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt']}
        comment_data.append(comment_data1)
      elif len(response['items'])==0:
        comment_data1={'comments':{}}
        comment_data.append(comment_data1)
  except:
    pass
  return comment_data
def final_channel_data(cid):
  #cid=input("Enter the channel id: ")
  ch=channel_data(cid)
  p=playlist_data(cid)
  v=video_data(cid)
  co=comments_data(cid)
  youtube_channel_data=({"channel_data":ch,"playlist_data":p,"video_data":v,"comments_data":co})
  return youtube_channel_data
client=pymongo.MongoClient("mongodb+srv://pjkavitha249:kavitha123@kavi.u9m57kk.mongodb.net/?retryWrites=true&w=majority")
vb=client["youtube"]
col=vb["channel_info"]


# insert data into mongo db
def insertinto_mongo():
    x=final_channel_data(cid)
    col.insert_one(x)
    return "inserted successfully"

#insertinto_mongo()

# creating channel data table in postgre and inserting values from mongodb
def channel_datatable():
  mydb=psycopg2.connect(host="localhost",user="postgres",password="1234",database="youtube",port="5432")
  cursor=mydb.cursor()
  try:
    create_query='''create table if not exists channel_data(channel_name varchar(100),
                                                          channel_id varchar(100) primary key,
                                                          subscriber_count bigint,
                                                          view_count bigint,
                                                          description text,
                                                          playlist_id varchar(100))'''
    cursor.execute(create_query)
  except:
    print("channel_data table already created")
  delete_query="delete from channel_data"
  cursor.execute(delete_query)
  
  eachchannel_list=[]
  for chd in col.find({},{"_id":0,"channel_data":1}):
     eachchannel_list.append(chd['channel_data'])
     df=pd.DataFrame(eachchannel_list)
  for index,row in df.iterrows():
      insert_query='''insert into channel_data (channel_name,
                                                channel_id,
                                                subscriber_count,
                                                view_count,
                                                description,
                                                playlist_id)
                                                values(%s,%s,%s,%s,%s,%s)'''
      values=(row['channel_name'],
              row['channel_id'],
              row['subscriber_count'],
              row['view_count'],
              row['description'],
              row['playlist_id'])
      try:
        cursor.execute(insert_query,values)
        mydb.commit()
      except:
        print("channels values are already inserted")
channel_datatable()

#creating playlist data table in postgre
def playlist_datatable():
    mydb=psycopg2.connect(host="localhost",user="postgres",password="1234",database="youtube",port="5432")
    cursor=mydb.cursor()
    drop_query="drop table if exists playlist_data"
    cursor.execute(drop_query)
    create1_query='''create table if not exists playlist_data(playlist_id varchar(100) primary key,
                                                                channel_id varchar(300),
                                                                playlist_name varchar(500))'''
    cursor.execute(create1_query)
    mydb.commit()
    eachplaylist_detail=[]
    col=vb["channel_info"]
    for pld in col.find({},{"_id":0,"playlist_data":1}):
        for i in range(len(pld['playlist_data'])):
            eachplaylist_detail.append(pld['playlist_data'][i])
    df1=pd.DataFrame(eachplaylist_detail)
    for index,row in df1.iterrows():
        insert1_query='''insert into playlist_data(playlist_id,
                                                    channel_id,
                                                    playlist_name)
                                                    values(%s,%s,%s)'''
        values=(row['playlist_id'],
                row['channel_id'],
                row['playlist_name'])
        try:
          cursor.execute(insert1_query,values)
          mydb.commit()
        except:
           pass

playlist_datatable()

#creating video data table in postgre
def video_datatable():
    mydb=psycopg2.connect(host="localhost",user="postgres",password="1234",database="youtube",port="5432")
    cursor=mydb.cursor()
    drop_query="drop table if exists video_data"
    cursor.execute(drop_query)
    mydb.commit()
    create2_query='''create table if not exists video_data(video_id varchar(100) primary key,
                                                            video_name varchar(100),
                                                            channel_id varchar(100),
                                                            video_description text,
                                                            tags text,
                                                            published_at timestamp,
                                                            view_count bigint,
                                                            like_count bigint,
                                                            favorite_count bigint,
                                                            comment_count bigint,
                                                            duration varchar(100),
                                                            thumbnails text,
                                                            caption_status varchar(50))'''
    cursor.execute(create2_query)
    mydb.commit()
    delete2_query="delete from video_data"
    cursor.execute(delete2_query)
    eachvideo_detail=[]
    col=vb["channel_info"]
    for vid in col.find({},{"_id":0,"video_data":1}):
        for i in range(len(vid['video_data'])): 
            eachvideo_detail.append(vid["video_data"][i])
    df2=pd.DataFrame(eachvideo_detail)

    for index,row in df2.iterrows():
        insert2_query='''insert into video_data(video_id,
                                                video_name,
                                                channel_id,
                                                video_description,
                                                tags,
                                                published_at,
                                                view_count,
                                                like_count,
                                                favorite_count,
                                                comment_count,
                                                duration,
                                                thumbnails,
                                                caption_status)
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['video_id'],
                row['video_name'],
                row['channel_id'],
                row['video_description'],
                row['tags'],
                row['published_at'],
                row['view_count'],
                row['like_count'],
                row['favorite_count'],
                row['comment_count'],
                row['duration'],
                row['thumbnails'],
                row['caption_status']) 
        try:                                           
          cursor.execute(insert2_query,values)
          mydb.commit()
        except:
           pass

video_datatable()

#creating comments data table in postgre
def comments_datatable():
    mydb=psycopg2.connect(host="localhost",user="postgres",password="1234",database="youtube",port="5432")
    cursor=mydb.cursor()
    drop_query="drop table if exists comments_data"
    cursor.execute(drop_query)
    mydb.commit()
    create3_query='''create table if not exists comments_data(channel_id varchar(100),
                                                            video_id varchar(100),
                                                            comment_id varchar(100) primary key,
                                                            comment_text text,
                                                            comment_author varchar(50),
                                                            comment_published_at varchar(100))'''


    cursor.execute(create3_query)
    delete3_query="delete from comments_data"
    cursor.execute(delete3_query)
    eachcomment_detail=[]
    col=vb["channel_info"]
    for cmd in col.find({},{"_id":0,"comments_data":1}):
        for i in range(len(cmd['comments_data'])):
            eachcomment_detail.append(cmd["comments_data"][i])
    df3=pd.DataFrame(eachcomment_detail)

    for index,row in df3.iterrows():
        insert3_query='''insert into comments_data(channel_id,
                                                    video_id,
                                                    comment_id,
                                                    comment_text,
                                                    comment_author,
                                                    comment_published_at)
                                                    values(%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_id'],
                row['video_id'],
                row['comment_id'],
                row['comment_text'],
                row['comment_author'],
                row['comment_published_at'])
        try:
            cursor.execute(insert3_query,values)
            mydb.commit()
        except:
            pass
        
comments_datatable()

def pgsql_tables():
    channel_datatable()
    playlist_datatable()
    video_datatable()
    comments_datatable()
    return "Tables created and values inserted in pgsql successfully"

#channel table for streamlit dataframe
def st_show_channeldata():
    eachchannel_list=[]
    col=vb["channel_info"]
    for chd in col.find({},{"_id":0,"channel_data":1}):
        eachchannel_list.append(chd['channel_data'])
    df=st.dataframe(eachchannel_list)
    return df
 
#playlist table for streamlit dataframe
def st_show_playlistdata():
    eachplaylist_detail=[]
    col=vb["channel_info"]
    for pld in col.find({},{"_id":0,"playlist_data":1}):
        for i in range(len(pld['playlist_data'])):
            eachplaylist_detail.append(pld['playlist_data'][i])
    df1=st.dataframe(eachplaylist_detail)
    return df1

#video table for streamlit dataframe
def st_show_videodata():
    eachvideo_detail=[]
    col=vb["channel_info"]
    for vid in col.find({},{"_id":0,"video_data":1}):
        for i in range(len(vid['video_data'])): 
            eachvideo_detail.append(vid["video_data"][i])
    df2=st.dataframe(eachvideo_detail)
    return df2

#comments table for streamlit dataframe
def st_show_commentsdata():
    eachcomment_detail=[]
    col=vb["channel_info"]
    for cmd in col.find({},{"_id":0,"comments_data":1}):
        for i in range(len(cmd['comments_data'])):
            eachcomment_detail.append(cmd["comments_data"][i])
    df3=st.dataframe(eachcomment_detail)
    return df3

# creating streamlit application
st.title(':blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]')

cid=st.text_input("Enter the channel id")
if st.button("collect and insert data"):
    cid_list=[]
    vb=client["youtube"]
    col=vb['channel_info']
    for i in col.find({},{"_id":0,"channel_data":1}):
        cid_list.append(i['channel_data']['channel_id'])
    if cid in cid_list:
        st.success("Channel id already exists.")
    else:
        insert=insertinto_mongo()
        st.success(insert)


if st.button("Transfer to pgsql"):
    pg_tables=pgsql_tables()
    st.success(pg_tables)
    
show_table=st.radio("Select the table",("Channel","Playlist","Video","Comment"))

if show_table=="Channel":
    st_show_channeldata()
if show_table=="Playlist":
    st_show_playlistdata()
if show_table=="Video":
    st_show_videodata()
if show_table=="Comment":
    st_show_commentsdata()


# pgsql connection
mydb=psycopg2.connect(host="localhost",user="postgres",password="1234",database="youtube",port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select question",("1.What are the names of all the videos and their corresponding channels?",
                                         "2.Which channels have the most number of videos, and how many videos do they have?",
                                         "3.What are the top 10 most viewed videos and their respective channels?",
                                         "4.How many comments were made on each video, and what are their corresponding video names?",
                                         "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                         "6.What is the total number of likes and dislikes for each video and what are their corresponding video names?",
                                         "7.What is the total number of views for each channel and what are their corresponding channel names?",
                                         "8.What are the names of all the channels that have published videos in the year 2022?",
                                         "9.What is the average duration of all videos in each channel and what are their corresponding channel names?",
                                         "10.Which videos have the highest number of comments and what are their corresponding channel names?"))

#writing pgsql queries for the 10 questions
mydb=psycopg2.connect(host="localhost",user="postgres",password="1234",database="youtube",port="5432")
cursor=mydb.cursor()
if question=="1.What are the names of all the videos and their corresponding channels?":
    query1='''select channel_data.channel_name,video_data.video_name 
            from channel_data inner join video_data on channel_data.channel_id=video_data.channel_id'''
    cursor.execute(query1)
    mydb.commit()
    tab1=cursor.fetchall()
    df1=pd.DataFrame(tab1,columns=['channel_name','video_name'])
    st.write(df1)
elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2='''select b.channel_name,a.total_video from	
            (select channel_id,count(playlist_id) as total_video 
	        from playlist_data 
	        group by channel_id 
	        order by count(playlist_id) desc) a
	        inner join channel_data b
	        on a.channel_id=b.channel_id'''
    cursor.execute(query2)
    mydb.commit()
    tab2=cursor.fetchall()
    df2=pd.DataFrame(tab2,columns=['channel_name','total_video'])
    st.write(df2)
elif question=="3.What are the top 10 most viewed videos and their respective channels?":
    query3='''select b.channel_name,a.video_name,a.view_count from
        (select channel_id,video_name,view_count 
        from video_data 
        order by view_count desc 
        limit 10) as a 
        inner join channel_data as b
        on a.channel_id=b.channel_id'''
    cursor.execute(query3)
    mydb.commit()
    tab3=cursor.fetchall()
    df3=pd.DataFrame(tab3,columns=['channel_name','video_name','view_count'])
    st.write(df3)
elif question=="4.How many comments were made on each video, and what are their corresponding video names?":
    query4='''select video_name,comment_count from video_data where comment_count > 0'''
    cursor.execute(query4)
    mydb.commit()
    tab4=cursor.fetchall()
    df4=pd.DataFrame(tab4,columns=['video_name','comment_count'])
    st.write(df4)
elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select b.channel_name,a.video_name,a.like_count 
                from(select channel_id,video_name,like_count from video_data order by like_count desc) as a
                inner join channel_data as b 
                on a.channel_id=b.channel_id'''
    cursor.execute(query5)
    mydb.commit()
    tab5=cursor.fetchall()
    df5=pd.DataFrame(tab5,columns=['channel_name','video_name','like_count'])
    st.write(df5)
elif question=="6.What is the total number of likes and dislikes for each video and what are their corresponding video names?":
    query6='''select video_name,like_count from video_data'''
    cursor.execute(query6)
    mydb.commit()
    tab6=cursor.fetchall()
    df6=pd.DataFrame(tab6,columns=['video_name','like_count'])
    st.write(df6)
elif question=="7.What is the total number of views for each channel and what are their corresponding channel names?":
    query7='''select b.channel_name,a.video_name,a.view_count 
                from(select channel_id,video_name,view_count from video_data) as a
                inner join channel_data as b 
                on a.channel_id=b.channel_id'''
    cursor.execute(query7)
    mydb.commit()
    tab7=cursor.fetchall()
    df7=pd.DataFrame(tab7,columns=['channel_name','video_name','view_count'])
    st.write(df7)
elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8='''select video_name,published_at 
                from video_data 
                where extract (year from published_at)=2022'''
    cursor.execute(query8)
    mydb.commit()
    tab8=cursor.fetchall()
    df8=pd.DataFrame(tab8,columns=['video_name','published_at'])
    st.write(df8)
elif question=="9.What is the average duration of all videos in each channel and what are their corresponding channel names?":
    query9='''select h.channel_name,g.avg_duration
                from(select f.channel_id,avg(f.duration_int)as avg_duration	
                from(select channel_id,cast(e.new_duration as float)as duration_int
                from(select channel_id,concat(d.n_minute,'.',d.n_second)as new_duration
                    from(select channel_id,c.n_minute,regexp_replace(c.second,'\D','','g')as n_second
                        from (select channel_id,b.second,regexp_replace(b.minute,'\D','','g')as n_minute
                        from(select channel_id,split_part(a.m,'PT',2)as minute,
                    split_part(a.s,'S',1)as second
                    from(select channel_id,split_part(duration, 'M',1)as m,
                split_part(duration,'M',2)as s
                from video_data)as a)as b)as c)as d)as e)as f
                    group by f.channel_id)as g
                    inner join channel_data as h
                    on g.channel_id=h.channel_id'''
    cursor.execute(query9)
    mydb.commit()
    tab9=cursor.fetchall()
    df9=pd.DataFrame(tab9,columns=['channel_name','avg_duration'])
    st.write(df9)
elif question=="10.Which videos have the highest number of comments and what are their corresponding channel names?":
    query10='''select b.channel_name,a.comment_count	
                from(select channel_id,comment_count from video_data)as a
                    inner join channel_data as b
                    on a.channel_id=b.channel_id
                    order by comment_count desc'''
    cursor.execute(query10)
    mydb.commit()
    tab10=cursor.fetchall()
    df10=pd.DataFrame(tab10,columns=['channel_name','comment_count'])
    st.write(df10)

            
    