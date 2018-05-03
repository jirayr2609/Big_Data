from pyspark import SparkContext

if __name__ == '__main__':
	sc = SparkContext()

	taxi = sc.textFile('yellow.csv.gz')
	bike = sc.textFile('citibike.csv')
	list(enumerate(bike.take(1)[0].split(',')))
	
	def filterBike(pId, lines):
	    import csv
	    for row in csv.reader(lines):
		if (row[6]=='Greenwich Ave & 8 Ave' and 
		    row[3].startswith('2015-02-01')):
		    yield (row[3][:19])		    
	gBike = bike.mapPartitionsWithIndex(filterBike).cache()
	gBike.take(5)
	
	bike.filter(lambda x: 'Greenwich Ave & 8 Ave' in x).first()
	
	gLoc = (40.73901691, -74.00263761)
	
	import pyproj
	
	def filterTaxi(pId, lines):
	    if pId==0:
		next(lines)
	    import csv
	    import pyproj
	    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
	    gLoc = proj(-74.00263761, 40.73901691)
	    sqm = 1320**2
	    for row in csv.reader(lines):
		try:
		    dropoff = proj(float(row[5]), float(row[4]))
		except:
		    continue
		sDistance = (dropoff[0]-gLoc[0])**2 + (dropoff[1]-gLoc[1])**2
		if sDistance<sqm:
		    yield row[1][:19]
		    
	gTaxi = taxi.mapPartitionsWithIndex(filterTaxi).cache()
	gTaxi.take(5)
	
	lBike = gBike.collect()
	lTaxi = gTaxi.collect()
	
	import datetime
	count = 0
	for b in lBike:
	    bt = datetime.datetime.strptime(b,'%Y-%m-%d %H:%M:%S')
	    for t in lTaxi:
		tt = datetime.datetime.strptime(t,'%Y-%m-%d %H:%M:%S')
		diff = (bt-tt).total_seconds()
		if diff>0 and diff<600:
		    count+=1
		    break
	sCount = str(count)
	file = open("Lab10Solution.txt","w")
	file.write(sCount)
	file.close()

