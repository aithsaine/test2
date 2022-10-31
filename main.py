from pymongo import MongoClient

cnx = MongoClient("localhost",27017)

db = cnx["aviation"]
pilote_collection = db["pilote"]
vol_collection = db["vol"]
avion_collection = db["avion"]
def FindPilote(id:int):
    res = list(pilote_collection.find({"num_pilote":id}))
    for item in res:
        for key,value in dict(item).items():
            print(key,value,sep="  :  ",end="\n")

            
#FindPilote(3)

def ListerCountVolsForeEachPilote():
    res = list(vol_collection.aggregate([{'$group':{'_id':'$pilote',"CountVols":{"$sum":1}}},{"$match":{'CountVols':{'$gt':1}}},{"$project":{"_id":1,'CountVols':1}},{"$sort":{"_id":1}}]))
    if (len(res)):
        for item in res:
            for key,value in dict(item).items():
                print(key,value,sep=" : ",end="   ")
            print('\n')
    else:
        print("code pilote est incorrect")
# ListerCountVolsForeEachPilote()

def searshMaxVolsCount():
   res = list(vol_collection.aggregate([{'$group':{'_id':'$pilote',"countVols":{"$sum":1}}},{"$match":{'countVols':{"$max":'$countVols'}}},{'$sort':{"_id":1}}]))
   print(res)
# searshMaxVolsCount()


min =dict(list(avion_collection.aggregate([{"$group":{"_id":"null","min_capacite":{"$min":"$capacite"}}}]))[0])["min_capacite"]
result = avion_collection.find_one({"capacite":int(min)})
# print(result)
def searchListPilsParAvion(id_avion):
    res = list(vol_collection.aggregate([{"$group":{"_id":"$avion",'liste_pilotes':{"$push":{"av":"$pilote"}}}},{"$match":{"_id":id_avion}}]))
    if len (res):
        res2 = list(res[0]["liste_pilotes"])
        re = res2.copy()
        for i in range (len(re)-1):
         for j in range (i+1,len(re)-1):
            if re[i]["av"]==re[j]["av"]:
                re.remove(re[j])
        return re
                
    else:
        return "pas d'avion par ce numero"
print(searchListPilsParAvion(107))