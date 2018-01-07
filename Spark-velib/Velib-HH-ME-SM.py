###############################################################################
#  TP realise par Hamza Haloui, Mohammed El Abridi
#  et Stephane Multon
#  Decembre 2017
###############################################################################



from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

### Initialisation ###

sc = SparkContext.getOrCreate()
streaming = StreamingContext(sc, 5)

# Creation de l'emplacement du checkpoint
streaming.checkpoint("./temp")



################################ Question 1 ###################################

# data recueille les données texte
data = streaming.socketTextStream("velib.behmo.com", 9999)

# utils recueille les données utiles que nous utiliserons pour le reste de l'application ie les clés name, contract_name et available_bikes
utils = data.map(lambda theta: json.loads(theta))
utils = utils.map(lambda theta:(theta['contract_name'] + ', ' + theta['name'], theta['available_bikes']))




################################ Question 2 ###################################

# Affichage des stations vides toutes les 5 secondes

utils_vides = utils.filter(lambda theta: theta[1]==0)
utils_vides.pprint() 




################################ Question 3 ###################################

# Affichage des NOUVELLES stations vides toutes les 5 secondes

# La updateFunction est utilisee dans updateStateByKey

def updateFunction(newValues, runningCount):
    if not newValues:
        runningCount = 1
    else:
        if runningCount is None:
            runningCount = 1
        elif newValues[0] == 0 and runningCount == 1:
            runningCount = 0
        elif newValues[0] == 0 and runningCount == 0:
            runningCount = 1
        else:
            pass
    return runningCount

countStream = utils.reduceByKey(lambda x,y: x)
totalCounts = countStream.updateStateByKey(updateFunction)
newEmptyStations = totalCounts.filter(lambda theta: theta[1] == 0)

newEmptyStations.pprint()

###############################################################################


streaming.start()
streaming.awaitTermination()