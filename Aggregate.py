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
        total.append(twitter[66+i]+tumblr[66+i]+insta[66+i]+reddit[66+i]+youtube[66+i])
    del total[0],twitter[0],insta[0],youtube[0],reddit[0],tumblr[0]
    return total,twitter,reddit,youtube,insta,tumblr

def mentions(q):
    Sentiment=["Positive","Negative","Neutral"]
    youtube=[[],[],[],[]]
    reddit=[[],[],[],[]]
    tumblr=[[],[],[],[]]
    insta=[[],[],[],[]]
    twitter=[[],[],[],[]]
    total=[[],[],[],[]]
    for i in range (3):
        sentiment = Sentiment[i]
        total[i],twitter[i],reddit[i],youtube[i],insta[i],tumblr[i]=sentiment_count(q,sentiment)
    for i in range (len(youtube[0])):
        youtube[3].append(youtube[0][i]+youtube[1][i]+youtube[2][i])
        insta[3].append(insta[0][i]+insta[1][i]+insta[2][i])
        total[3].append(total[0][i]+total[1][i]+total[2][i])
        twitter[3].append(twitter[0][i]+twitter[1][i]+twitter[2][i])
        tumblr[3].append(tumblr[0][i]+tumblr[1][i]+tumblr[2][i])
        reddit[3].append(reddit[0][i]+reddit[1][i]+reddit[2][i])


    counts={
        'tag':q,
        'all_mentions':{
            'total':{'hourly': total[3][0:24],
                 'daily':total[3][24:54],
                 'monthly':total[3][54:66],
                 'yearly':total[3][66:72]},
        'twitter':{'hourly': twitter[3][0:24],
                 'daily':twitter[3][24:54],
                 'monthly':twitter[3][54:66],
                 'yearly':twitter[3][66:72]},
        'youtube':{'hourly': youtube[3][0:24],
                 'daily':youtube[3][24:54],
                 'monthly':youtube[3][54:66],
                 'yearly':youtube[3][66:72]},
        'reddit':{'hourly': reddit[3][0:24],
                 'daily':reddit[3][24:54],
                 'monthly':reddit[3][54:66],
                 'yearly':reddit[3][66:72]},
        'tumblr':{'hourly': tumblr[3][0:24],
                 'daily':tumblr[3][24:54],
                 'monthly':tumblr[3][54:66],
                 'yearly':tumblr[3][66:72]},
        'insta':{'hourly': insta[3][0:24],
                 'daily':insta[3][24:54],
                 'monthly':insta[3][54:66],
                 'yearly':insta[3][66:72]}
        },
        'positive_mentions':{
            'total':{'hourly': total[0][0:24],
                 'daily':total[0][24:54],
                 'monthly':total[0][54:66],
                 'yearly':total[0][66:72]},
        'twitter':{'hourly': twitter[0][0:24],
                 'daily':twitter[0][24:54],
                 'monthly':twitter[0][54:66],
                 'yearly':twitter[0][66:72]},
        'youtube':{'hourly': youtube[0][0:24],
                 'daily':youtube[0][24:54],
                 'monthly':youtube[0][54:66],
                 'yearly':youtube[0][66:72]},
        'reddit':{'hourly': reddit[0][0:24],
                 'daily':reddit[0][24:54],
                 'monthly':reddit[0][54:66],
                 'yearly':reddit[0][66:72]},
        'tumblr':{'hourly': tumblr[0][0:24],
                 'daily':tumblr[0][24:54],
                 'monthly':tumblr[0][54:66],
                 'yearly':tumblr[0][66:72]},
        'insta':{'hourly': insta[0][0:24],
                 'daily':insta[0][24:54],
                 'monthly':insta[0][54:66],
                 'yearly':insta[0][66:72]}
        },
        'negative_mentions':{
            'total':{'hourly': total[1][0:24],
                 'daily':total[1][24:54],
                 'monthly':total[1][54:66],
                 'yearly':total[1][66:72]},
        'twitter':{'hourly': twitter[1][0:24],
                 'daily':twitter[1][24:54],
                 'monthly':twitter[1][54:66],
                 'yearly':twitter[1][66:72]},
        'youtube':{'hourly': youtube[1][0:24],
                 'daily':youtube[1][24:54],
                 'monthly':youtube[1][54:66],
                 'yearly':youtube[1][66:72]},
        'reddit':{'hourly': reddit[1][0:24],
                 'daily':reddit[1][24:54],
                 'monthly':reddit[1][54:66],
                 'yearly':reddit[1][66:72]},
        'tumblr':{'hourly': tumblr[1][0:24],
                 'daily':tumblr[1][24:54],
                 'monthly':tumblr[1][54:66],
                 'yearly':tumblr[1][66:72]},
        'insta':{'hourly': insta[1][0:24],
                 'daily':insta[1][24:54],
                 'monthly':insta[1][54:66],
                 'yearly':insta[1][66:72]}
        },
        'neutral_mentions':{
            'total':{'hourly': total[2][0:24],
                 'daily':total[2][24:54],
                 'monthly':total[2][54:66],
                 'yearly':total[2][66:72]},
        'twitter':{'hourly': twitter[2][0:24],
                 'daily':twitter[2][24:54],
                 'monthly':twitter[2][54:66],
                 'yearly':twitter[2][66:72]},
        'youtube':{'hourly': youtube[2][0:24],
                 'daily':youtube[2][24:54],
                 'monthly':youtube[2][54:66],
                 'yearly':youtube[2][66:72]},
        'reddit':{'hourly': reddit[2][0:24],
                 'daily':reddit[2][24:54],
                 'monthly':reddit[2][54:66],
                 'yearly':reddit[2][66:72]},
        'tumblr':{'hourly': tumblr[2][0:24],
                 'daily':tumblr[2][24:54],
                 'monthly':tumblr[2][54:66],
                 'yearly':tumblr[2][66:72]},
        'insta':{'hourly': insta[2][0:24],
                 'daily':insta[2][24:54],
                 'monthly':insta[2][54:66],
                 'yearly':insta[2][66:72]}
        }
    }
    return counts
    
