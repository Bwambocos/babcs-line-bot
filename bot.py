import os
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import dropbox
from dropbox.files import WriteMode
import pickle
import requests
from requests.exceptions import ConnectTimeout
from bs4 import BeautifulSoup
from linebot import LineBotApi
from linebot.models import TextSendMessage
import datetime
import time
import gc

sched = BlockingScheduler(
    executors = {
        'threadpool' : ThreadPoolExecutor(max_workers = 2),
        'processpool' : ProcessPoolExecutor(max_workers = 2)
    }
)

@sched.scheduled_job('interval', minutes = 5, executor = 'threadpool')
def scheduled_job():

    print("notify_news: ----- Detect news update Start -----\n")

    data = []

    dbx = dropbox.Dropbox(os.environ["DROPBOX_KEY"])
    dbx.users_get_current_account()
    
    # Download
    dbx.files_download_to_file("newsData.txt", "/UT/newsData.txt")
    with open("newsData.txt", "rb") as f:
        data = pickle.load(f)
        del f
        gc.collect()
    
    # Detect updates
    pageHTML = requests.get("https://www.c.u-tokyo.ac.jp/zenki/news/index.html")
    results = []
    try:
        pageHTML.raise_for_status()
        pageData = BeautifulSoup(pageHTML.text, "html.parser")
        pageDiv = pageData.find_all(id = "newslist2")
        pageDates = pageDiv[0].find_all("dt")
        pageTitles = pageDiv[0].find_all("dd")
    
        nums = len(pageDates)
        index = 0
        newData = []
        while index < nums:
            if (len(pageTitles[index].contents) == 3 and pageTitles[index].contents[2].attrs["src"] == "/zenki/news/kyoumu/images/common/news_important2.gif") or (len(pageTitles[index].contents) == 5 and pageTitles[index].contents[4].attrs["src"] == "/zenki/news/kyoumu/images/common/news_important2.gif") or (len(pageTitles[index].contents) == 5 and pageTitles[index].contents[3].attrs["src"] == "/zenki/news/kyoumu/images/common/news_important2.gif"):
                date = str(pageDates[index].contents[0])
                title = str(pageTitles[index].contents[0].contents[0])
                url = str(pageTitles[index].contents[0].attrs["href"])
                if url[0] != 'h':
                    url = "https://www.c.u-tokyo.ac.jp" + url
                newData.append((date, title, url))
            index += 1
    
        for row in newData:
            if row not in data:
                results.append(row)
    except:
        print("notify_news: pageHTML Error")
        return
    print("notify_news: Detected " + str(len(results)) + " updates\n")

    # Send LINE messages
    line_bot_api = LineBotApi(os.environ["CHANNEL_ACCESS_TOKEN"])
    for row in results:
        message = str("お知らせが更新されました：\n" + row[1] + "\n" + row[2])
        line_bot_api.broadcast(TextSendMessage(text = message))
        if os.environ["LINE_GROUP_ID"] != "NULL":
            line_bot_api.push_message(os.environ["LINE_GROUP_ID"], TextSendMessage(text = message))
        print("notify_news: Noticed new information (title : " + row[1] + ")\n")

    # Upload
    data = newData
    with open("newsData.txt", "wb") as f:
        pickle.dump(data, f)
        del f
        gc.collect()
    with open("newsData.txt", "rb") as f:
        dbx.files_upload(f.read(), "/UT/newsData.txt", mode = dropbox.files.WriteMode.overwrite)
        del f
        gc.collect()
    
    print("notify_news: ----- Detect news update End -----\n")
    
    del data
    del newData
    del pageHTML
    del results
    gc.collect()

def getStatus(bdName, bdNum):
    
    newRoomData = []

    while True:
        pageHTML = requests.get("https://wifi-monitor.nc.u-tokyo.ac.jp/" + bdName + ".html")
        pageHTML.raise_for_status()
        pageData = BeautifulSoup(pageHTML.content, "html.parser")
        rows = pageData.find_all("tr")

        for row in rows:
            name = str(row.contents[0].text)
            if name == "WiFi アクセスポイント設置場所":
                continue
            connections = int(row.contents[1].text)
            newRoomData.append((name, connections))

        if len(newRoomData) >= bdNum:
            break
        newRoomData.clear()
        time.sleep(10)

    print("statistics: Downloaded " + bdName + " status (" + str(len(newRoomData)) + " rooms)")
    return newRoomData

@sched.scheduled_job('interval', minutes = 15, executor = 'threadpool')
def scheduled_job():

    bdData = []
    roomData = []
    
    print("statistics: ----- Update statistics Start -----\n")

    dbx = dropbox.Dropbox(os.environ["DROPBOX_KEY"])
    dbx.users_get_current_account()
    
    # Download
    dbx.files_download_to_file("bdData.txt", "/UT/bdData.txt")
    with open("bdData.txt", "rb") as f:
        bdData = pickle.load(f)
        del f
        gc.collect()
    dbx.files_download_to_file("roomData.txt", "/UT/roomData.txt")
    with open("roomData.txt", "rb") as f:
        roomData = pickle.load(f)
        del f
        gc.collect()

    # Update statistics
    addedRoomData = []
    for bd in bdData:
        bdStatus = getStatus(bd[0], bd[1])
        bd[1] = len(bdStatus)
        addedRoomData.extend(bdStatus)
    roomData.append((datetime.datetime.today(), addedRoomData))
    print("statistics: Updated statistics (" + str(len(roomData)) + " data)")

    # Upload
    with open("bdData.txt", "wb") as f:
        pickle.dump(bdData, f)
        del f
        gc.collect()
    with open("bdData.txt", "rb") as f:
        dbx.files_upload(f.read(), "/UT/bdData.txt", mode = dropbox.files.WriteMode.overwrite)
        del f
        gc.collect()
    with open("roomData.txt", "wb") as f:
        pickle.dump(roomData, f)
        del f
        gc.collect()
    with open("roomData.txt", "rb") as f:
        dbx.files_upload(f.read(), "/UT/roomData.txt", mode = dropbox.files.WriteMode.overwrite)
        del f
        gc.collect()

    print("statistics: ----- Update statistics End -----\n")

# Run
sched.start()
