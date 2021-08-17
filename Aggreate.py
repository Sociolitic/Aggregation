import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
YouTube = db.youTube
Twitter = db.twitter
Tumblr = db.tumblr
Instagram = db.instagram
Reddit = db.reddit

def mentions_count(q):
    now = datetime.now()
    current_date_=datetime.now
    youtube=[0]
    reddit=[0]
    tumblr=[0]
    insta=[0]
    twitter=[0]
    total=[0]
    current_date = now.strftime("%Y-%m-%d %H:%M:%S")
    for i in range (1,25):
        past_date = now - timedelta(hours = i)
        past_date_str=past_date.strftime('%Y-%m-%d %H:%M:%S')
        youtube.append(YouTube.count_documents( {'tag': q, "publishedTime":{'$gt': (past_date_str)}})-sum(youtube))
        twitter.append(Twitter.count_documents({'tag': q, "created_time":{'$gt':(past_date)}})-sum(twitter))
        tumblr.append(Tumblr.count_documents( { 'tag': q, "created_time":{'%gt':(past_date_str)}})-sum(tumblr))
        insta.append(Instagram.count_documents( {'tag': q, "publishedTime":{'%gt':(past_date_str)}})-sum(insta))
        reddit.append(Reddit.count_documents( {'tag': q, "created_time":{'%gt':(past_date_str)}})-sum(reddit))
        total.append(twitter[i]+tumblr[i]+insta[i]+reddit[i]+youtube[i])

    for i in range (1,31):
        past_date = now - timedelta(days = i)
        past_date_str=past_date.strftime('%Y-%m-%d %H:%M:%S')
        if(i>1):
            youtube.append(YouTube.count_documents( {'tag': q, "publishedTime":{'$gt': (past_date_str)}})-sum(youtube[25:24+i]))
            twitter.append(Twitter.count_documents({'tag': q, "created_time":{'$gt':(past_date)}})-sum(twitter[25:24+i]))
            tumblr.append(Tumblr.count_documents( { 'tag': q, "created_time":{'%gt':(past_date_str)}})-sum(tumblr[25:24+i]))
            insta.append(Instagram.count_documents( {'tag': q, "publishedTime":{'%gt':(past_date_str)}})-sum(insta[25:24+i]))
            reddit.append(Reddit.count_documents( {'tag': q, "created_time":{'%gt':(past_date_str)}})-sum(reddit[25:24+i]))
        else:
            youtube.append(YouTube.count_documents( {'tag': q, "publishedTime":{'$gt': (past_date_str)}}))
            twitter.append(Twitter.count_documents({'tag': q, "created_time":{'$gt':(past_date)}}))
            tumblr.append(Tumblr.count_documents( { 'tag': q, "created_time":{'%gt':(past_date_str)}}))
            insta.append(Instagram.count_documents( {'tag': q, "publishedTime":{'%gt':(past_date_str)}}))
            reddit.append(Reddit.count_documents( {'tag': q, "created_time":{'%gt':(past_date_str)}}))
        total.append(twitter[24+i]+tumblr[24+i]+insta[24+i]+reddit[24+i]+youtube[24+i])

    for i in range (1,13):
        past_date = now - relativedelta(months = i)
        past_date_str=past_date.strftime('%Y-%m-%d %H:%M:%S')
        if(i>1):
            youtube.append(YouTube.count_documents( {'tag': q, "publishedTime":{'$gt': (past_date_str)}})-sum(youtube[55:54+i]))
            twitter.append(Twitter.count_documents({'tag': q, "created_time":{'$gt':(past_date)}})-sum(twitter[55:54+i]))
            tumblr.append(Tumblr.count_documents( { 'tag': q, "created_time":{'%gt':(past_date_str)}})-sum(tumblr[55:54+i]))
            insta.append(Instagram.count_documents( {'tag': q, "publishedTime":{'%gt':(past_date_str)}})-sum(insta[55:54+i]))
            reddit.append(Reddit.count_documents( {'tag': q, "created_time":{'%gt':(past_date_str)}})-sum(reddit[55:54+i]))
        else:
            youtube.append(YouTube.count_documents( {'tag': q, "publishedTime":{'$gt': (past_date_str)}}))
            twitter.append(Twitter.count_documents({'tag': q, "created_time":{'$gt':(past_date)}}))
            tumblr.append(Tumblr.count_documents( { 'tag': q, "created_time":{'%gt':(past_date_str)}}))
            insta.append(Instagram.count_documents( {'tag': q, "publishedTime":{'%gt':(past_date_str)}}))
            reddit.append(Reddit.count_documents( {'tag': q, "created_time":{'%gt':(past_date_str)}}))
        total.append(twitter[54+i]+tumblr[54+i]+insta[54+i]+reddit[54+i]+youtube[54+i])
    for i in range (1,6):
        past_date = now - relativedelta(years = i)
        past_date_str=past_date.strftime('%Y-%m-%d %H:%M:%S')
        if(i>1):
            youtube.append(YouTube.count_documents( {'tag': q, "publishedTime":{'$gt': (past_date_str)}})-sum(youtube[67:66+i]))
            twitter.append(Twitter.count_documents({'tag': q, "created_time":{'$gt':(past_date)}})-sum(twitter[67:66+i]))
            tumblr.append(Tumblr.count_documents( { 'tag': q, "created_time":{'%gt':(past_date_str)}})-sum(tumblr[67:66+i]))
            insta.append(Instagram.count_documents( {'tag': q, "publishedTime":{'%gt':(past_date_str)}})-sum(insta[67:66+i]))
            reddit.append(Reddit.count_documents( {'tag': q, "created_time":{'%gt':(past_date_str)}})-sum(reddit[67:66+i]))
        else:
            youtube.append(YouTube.count_documents( {'tag': q, "publishedTime":{'$gt': (past_date_str)}}))
            twitter.append(Twitter.count_documents({'tag': q, "created_time":{'$gt':(past_date)}}))
            tumblr.append(Tumblr.count_documents( { 'tag': q, "created_time":{'%gt':(past_date_str)}}))
            insta.append(Instagram.count_documents( {'tag': q, "publishedTime":{'%gt':(past_date_str)}}))
            reddit.append(Reddit.count_documents( {'tag': q, "created_time":{'%gt':(past_date_str)}}))
        total.append(twitter[65+i]+tumblr[65+i]+insta[65+i]+reddit[65+i]+youtube[65+i])
    del total[0],twitter[0],insta[0],youtube[0],reddit[0],tumblr[0]
    counts={
        'tag':q,
        'total':{'hourly': total[0:24],
                 'daily':total[24:54],
                 'monthly':total[54:66],
                 'yearly':total[66:72]},
        'twitter':{'hourly': twitter[0:24],
                 'daily':twitter[24:54],
                 'monthly':twitter[54:66],
                 'yearly':twitter[66:72]},
        'youtube':{'hourly': youtube[0:24],
                 'daily':youtube[24:54],
                 'monthly':youtube[54:66],
                 'yearly':youtube[66:72]},
        'reddit':{'hourly': reddit[0:24],
                 'daily':reddit[24:54],
                 'monthly':reddit[54:66],
                 'yearly':reddit[66:72]},
        'tumblr':{'hourly': tumblr[0:24],
                 'daily':tumblr[24:54],
                 'monthly':tumblr[54:66],
                 'yearly':tumblr[66:72]},
        'insta':{'hourly': insta[0:24],
                 'daily':insta[24:54],
                 'monthly':insta[54:66],
                 'yearly':insta[66:72]}
    }
    return counts

def sentiment_count(q,sentiment):
    now = datetime.now()
    current_date_=datetime.now
    youtube=[0]
    reddit=[0]
    tumblr=[0]
    insta=[0]
    twitter=[0]
    total=[0]
    current_date = now.strftime("%Y-%m-%d %H:%M:%S")
    for i in range (1,25):
        past_date = now - timedelta(hours = i)
        past_date_str=past_date.strftime('%Y-%m-%d %H:%M:%S')
        youtube.append(YouTube.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'$gt': (past_date_str)}})-sum(youtube))
        twitter.append(Twitter.count_documents({'tag': q,'sentiment':sentiment, "created_time":{'$gt':(past_date)}})-sum(twitter))
        tumblr.append(Tumblr.count_documents( { 'tag': q, 'sentiment':sentiment,"created_time":{'%gt':(past_date_str)}})-sum(tumblr))
        insta.append(Instagram.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'%gt':(past_date_str)}})-sum(insta))
        reddit.append(Reddit.count_documents( {'tag': q, 'sentiment':sentiment,"created_time":{'%gt':(past_date_str)}})-sum(reddit))
        total.append(twitter[i]+tumblr[i]+insta[i]+reddit[i]+youtube[i])

    for i in range (1,31):
        past_date = now - timedelta(days = i)
        past_date_str=past_date.strftime('%Y-%m-%d %H:%M:%S')
        if(i>1):
            youtube.append(YouTube.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'$gt': (past_date_str)}})-sum(youtube[25:24+i]))
            twitter.append(Twitter.count_documents({'tag': q,'sentiment':sentiment, "created_time":{'$gt':(past_date)}})-sum(twitter[25:24+i]))
            tumblr.append(Tumblr.count_documents( { 'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}})-sum(tumblr[25:24+i]))
            insta.append(Instagram.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'%gt':(past_date_str)}})-sum(insta[25:24+i]))
            reddit.append(Reddit.count_documents( {'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}})-sum(reddit[25:24+i]))
        else:
            youtube.append(YouTube.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'$gt': (past_date_str)}}))
            twitter.append(Twitter.count_documents({'tag': q,'sentiment':sentiment, "created_time":{'$gt':(past_date)}}))
            tumblr.append(Tumblr.count_documents( { 'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}}))
            insta.append(Instagram.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'%gt':(past_date_str)}}))
            reddit.append(Reddit.count_documents( {'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}}))
        total.append(twitter[24+i]+tumblr[24+i]+insta[24+i]+reddit[24+i]+youtube[24+i])

    for i in range (1,13):
        past_date = now - relativedelta(months = i)
        past_date_str=past_date.strftime('%Y-%m-%d %H:%M:%S')
        if(i>1):
            youtube.append(YouTube.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'$gt': (past_date_str)}})-sum(youtube[55:54+i]))
            twitter.append(Twitter.count_documents({'tag': q,'sentiment':sentiment, "created_time":{'$gt':(past_date)}})-sum(twitter[55:54+i]))
            tumblr.append(Tumblr.count_documents( { 'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}})-sum(tumblr[55:54+i]))
            insta.append(Instagram.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'%gt':(past_date_str)}})-sum(insta[55:54+i]))
            reddit.append(Reddit.count_documents( {'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}})-sum(reddit[55:54+i]))
        else:
            youtube.append(YouTube.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'$gt': (past_date_str)}}))
            twitter.append(Twitter.count_documents({'tag': q, 'sentiment':sentiment,"created_time":{'$gt':(past_date)}}))
            tumblr.append(Tumblr.count_documents( { 'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}}))
            insta.append(Instagram.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'%gt':(past_date_str)}}))
            reddit.append(Reddit.count_documents( {'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}}))
        total.append(twitter[54+i]+tumblr[54+i]+insta[54+i]+reddit[54+i]+youtube[54+i])
    for i in range (1,6):
        past_date = now - relativedelta(years = i)
        past_date_str=past_date.strftime('%Y-%m-%d %H:%M:%S')
        if(i>1):
            youtube.append(YouTube.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'$gt': (past_date_str)}})-sum(youtube[67:66+i]))
            twitter.append(Twitter.count_documents({'tag': q,'sentiment':sentiment, "created_time":{'$gt':(past_date)}})-sum(twitter[67:66+i]))
            tumblr.append(Tumblr.count_documents( { 'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}})-sum(tumblr[67:66+i]))
            insta.append(Instagram.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'%gt':(past_date_str)}})-sum(insta[67:66+i]))
            reddit.append(Reddit.count_documents( {'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}})-sum(reddit[67:66+i]))
        else:
            youtube.append(YouTube.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'$gt': (past_date_str)}}))
            twitter.append(Twitter.count_documents({'tag': q, 'sentiment':sentiment,"created_time":{'$gt':(past_date)}}))
            tumblr.append(Tumblr.count_documents( { 'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}}))
            insta.append(Instagram.count_documents( {'tag': q,'sentiment':sentiment, "publishedTime":{'%gt':(past_date_str)}}))
            reddit.append(Reddit.count_documents( {'tag': q,'sentiment':sentiment, "created_time":{'%gt':(past_date_str)}}))
        total.append(twitter[65+i]+tumblr[65+i]+insta[65+i]+reddit[65+i]+youtube[65+i])
    del total[0],twitter[0],insta[0],youtube[0],reddit[0],tumblr[0]
    counts={
        'tag':q,
        'total':{'hourly': total[0:24],
                 'daily':total[24:54],
                 'monthly':total[54:66],
                 'yearly':total[66:72]},
        'twitter':{'hourly': twitter[0:24],
                 'daily':twitter[24:54],
                 'monthly':twitter[54:66],
                 'yearly':twitter[66:72]},
        'youtube':{'hourly': youtube[0:24],
                 'daily':youtube[24:54],
                 'monthly':youtube[54:66],
                 'yearly':youtube[66:72]},
        'reddit':{'hourly': reddit[0:24],
                 'daily':reddit[24:54],
                 'monthly':reddit[54:66],
                 'yearly':reddit[66:72]},
        'tumblr':{'hourly': tumblr[0:24],
                 'daily':tumblr[24:54],
                 'monthly':tumblr[54:66],
                 'yearly':tumblr[66:72]},
        'insta':{'hourly': insta[0:24],
                 'daily':insta[24:54],
                 'monthly':insta[54:66],
                 'yearly':insta[66:72]}
    }
    return counts
