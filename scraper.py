from flask import Flask, render_template, request, jsonify, app
from flask_cors import CORS,cross_origin

import os
import time

import requests

from bs4 import BeautifulSoup
from selenium import webdriver
from pytube import YouTube
from selenium.webdriver.common.keys import Keys

import pymongo


import mysql.connector as conn
from awscreds import AWSAccessKeyId,AWSSecretKey
import boto3
import pandas as pd
import numpy as np


app = Flask(__name__)
@app.route('/',methods=['GET'])
@cross_origin()
def homePage():
    return render_template("index.html")

@app.route('/scrape',methods=['POST','GET'])
@cross_origin()
def index():
    if request.method == 'POST':
        url = request.form.get('link')
        # mysql connection
        mydb = conn.connect(host = "localhost", user = 'root', passwd = "gopal")
        cursor = mydb.cursor()
        cursor.execute('create database IF NOT EXISTS youtube_data;')
        cursor.execute("USE youtube_data")
        cursor.execute("CREATE TABLE IF NOT EXISTS dataset(s_no int,channel_names varchar(20),video_links varchar(100),s3_videos varchar(100),titles varchar(100),likes varchar(10),no_of_comments varchar(20),thumbnail_links varchar(100))")

        s3 = boto3.client("s3", region_name='ap-south-1', aws_access_key_id=AWSAccessKeyId,
                                       aws_secret_access_key=AWSSecretKey)
        #mongoDB Connection
        client = pymongo.MongoClient("mongodb+srv://gopalakrishna97:Gopal123@krishna.wdftn.mongodb.net/?retryWrites=true&w=majority")
        db = client.test

        db1 = client['youtubeScraper']
        youtube_data_collection = db1['video_data']


        PATH = ".\chromedriver_win32\chromedriver.exe"
        driver = webdriver.Chrome(executable_path = PATH)

        driver.get(url+"/videos")
        driver.maximize_window()

        driver.execute_script("window.scrollBy(0,15000)")
        time.sleep(5)
        content = driver.page_source.encode('utf-8').strip()

        soup = BeautifulSoup(content,'lxml')
        videos = soup.find_all("a",id='video-title')


        # extracting 50 unique video links
        links = set()
        start_number = 0
        end_number = 50
        for video in videos[start_number:end_number]:
            link = video.get('href')
            # picking only full length video (NOT short videos)
            verify_link = link.split('=')
            if len(verify_link) == 2:
                links.add("https://www.youtube.com"+link)


        scraped_data = []
        count = 0
        for link in links:
            driver.get(link)
            # scrollHeight = driver.execute_script("return window.scrollMaxY")
            # scrolled_page = 0
            time.sleep(2)
            driver.execute_script("window.scrollBy(0,400)")
            time.sleep(2)

            html = driver.page_source.encode('utf-8').strip()
            # response_page = requests.get(link).content
            soup = BeautifulSoup(html, 'html5lib')

            video_link = link
            # video_link_list.append(video_link)
            try:
                channel_name = soup.find('yt-formatted-string', class_="style-scope ytd-channel-name").text  # working
                # channel_names.append(channel_name)
            except:
                time.sleep(3)
                continue

            try:
                title = soup.find('yt-formatted-string',class_="style-scope ytd-video-primary-info-renderer").text  # working
                # titles_list.append(title)
            except:
                time.sleep(3)
                continue
            try:
                likes = soup.find('a',class_="yt-simple-endpoint style-scope ytd-toggle-button-renderer").text # working
                # likes_list.append(likes)
            except:
                time.sleep(3)
                continue
            try:
                comments = soup.find('yt-formatted-string',class_="count-text style-scope ytd-comments-header-renderer").text #working
                # no_of_comments_list.append(comments)
            except:
                time.sleep(3)
                continue


            # title , thumbnail and comments for storing in mongoDB
            mongoDB_content = {"Tiltle": title,"comments": []}
            def get_thumbnail():
                try:
                    video_id = video_link.split('=')
                    thumbnail_link = 'https://i.ytimg.com/vi/{}/maxresdefault.jpg'.format(video_id[-1])
                    thumbnail = requests.get(thumbnail_link).content
                    # thumbnail_links_list.append(thumbnail_link)
                    mongoDB_content['thumbnail'] = thumbnail
                    folder_path = os.path.join('./images', channel_name+"images")
                    if not os.path.exists(folder_path):
                        os.makedirs(folder_path)
                    file_path = os.path.join(folder_path, '_'.join(title.lower().split(' ')[:2]) +"_"+ str(count)+"_thumbnail.jpg")
                    f = open(file_path, 'wb')
                    f.write(thumbnail)
                    print('image saved successfully')
                    f.close()
                    return thumbnail_link
                except Exception as e:
                    print(e)

            def get_video():

                link = video_link
                try:
                    yt = YouTube(link)
                except:
                    print("Connection Error for downloading video")

                try:
                    folder_path = os.path.join("./images", channel_name+"_videos")
                    if not os.path.exists(folder_path):
                        os.makedirs(folder_path)
                    file_name = '_'.join(title.lower().split(' ')[:2]) +"_"+str(count)+"_"+ "video.mp4"
                    yt.streams.get_by_itag(22).download(output_path=folder_path,filename=file_name)
                    print('Video Downloaded successfully!')

                except Exception as e:
                    print(e)
                try:
                    file_path = os.path.join(folder_path,file_name)
                    s3.upload_file(file_path,"youtubevideos1",file_name)
                    s3_video_link = "https://youtubevideos1.s3.ap-south-1.amazonaws.com/{}".format(file_name)
                    return s3_video_link
                    # s3_videos_list.append(s3_video_link)
                except Exception as e:
                    print(e)



            def get_comments():
                try:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    comments_Authors = soup.find_all('h3', class_="style-scope ytd-comment-renderer")
                    comment_texts = soup.find_all('yt-formatted-string', class_="style-scope ytd-comment-renderer")
                except:
                    comments_Authors = ["No comments",]
                    comment_texts = ["No commnets",]

                for (Author_tag,Comment_tag) in zip(comments_Authors,comment_texts):
                    author = Author_tag.text.strip()
                    comment = Comment_tag.text.strip()
                    dict = {"Author": author, "comment": comment}
                    mongoDB_content['comments'].append(dict)



            thumbnail_link = get_thumbnail()
            s3_video_link = get_video()
            get_comments()

            mydict = {"count":count,"channel_name":channel_name,"video_link":video_link,"s3_video_link":s3_video_link,"title":title,"likes":likes,"comments":comments,"thumbnail_link":thumbnail_link}
            scraped_data.append(mydict)
            cursor.execute("INSERT INTO dataset VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",(count,channel_name,video_link,s3_video_link,title,likes,comments,thumbnail_link))
            mydb.commit()
            #     inserting data to mongoDB
            try:
                youtube_data_collection.insert_one(mongoDB_content)
            except Exception as e:
                print(e)
            count += 1

        return render_template('results_page.html', scraped_data=scraped_data[0:(len(scraped_data)-1)])




if __name__ == "__main__":
    #app.run(host='127.0.0.1', port=8001, debug=True)
	app.run(debug=True)





