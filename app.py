from flask import Flask,redirect,url_for,render_template,request,jsonify
from flask.templating import render_template_string
from pymongo import MongoClient
import pandas as pd
import math
from dotenv import load_dotenv
import os


load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

app = Flask(__name__)

#connecting to mongodb
def _connect_mongo(host, port, username, password, db):
    if username and password:
        mongo_uri = DATABASE_URL
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[db]

#reading db and collection from mongodb using preset connection
def read_mongo(db, collection, host='localhost', port=27017, username=None, password=None):
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    cursor = db[collection].find()
    df =  pd.DataFrame(list(cursor))
    return df

#function to make recommendations on a given user_id
def recommend(target_uid):
    df=read_mongo("recommender", "dataset","host",27017,"username","password").drop(columns=["_id"],axis=1)
    uid_dist={}
    for i in range(len(df)):
        if(i == target_uid):
            continue
        uid_dist[i]=0
        for j in range(len(df.columns)):
            if((not math.isnan(df.iloc[target_uid,j])) and (not math.isnan(df.iloc[i,j]))):
                uid_dist[i]+=pow(abs(df.iloc[target_uid,j]-df.iloc[i,j]),4)
            else:
                uid_dist[i]+=pow(5,4)

    bound=int(round(math.sqrt(len(df)),0))
    uid_dist_sorted=sorted(uid_dist.items(), key=lambda x: x[1],reverse=True)[:bound]
    rating_target={}
    for i in range(len(df.columns)):
        col=df.iloc[:,i].name
        temp=0
        ct=0
        for j in range(len(uid_dist_sorted)):
            row=uid_dist_sorted[j][0]
            if math.isnan(df.iloc[target_uid,i]):
                if(not math.isnan(df.iloc[row,i])):
                    ct+=1
                    temp+=df.iloc[row,i]
            if(ct!=0):
                rating_target[col]=temp/ct

    result_tag=sorted(rating_target.items(), key=lambda x: x[1], reverse=True)[:5]

    return result_tag

@app.route("/")
def home():
    data = request.form.to_dict()
    userid = int(data["userid"])
    result = recommend(userid)
    
    return jsonify(result)

if __name__ == "__main__":
    app.run()
