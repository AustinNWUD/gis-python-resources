# Computes a distance table for all points in a selection set

import caliperpy
import tempfile

def compute_distance_table(dk, opts):
   
    # Create and configure the routing object
    router = dk.CreateGisdkObject("gis_ui", "Routing.Router")
    router.Minimize = "Time"
    router.IncludeRestStops = True
    router.TimeBetweenRests = 5  # minutes
    router.RestStopDuration = 20  # minutes
    router.FuelPrice = 3.29
    router.SingleThreaded = True

    # Speed levels: 1=slow, 4=normal, 7=fast
    router.MajorHighwaySpeedLevel = 4
    router.SecondaryHighwaySpeedLevel = 4
    router.LocalHighwaySpeedLevel = 4
    router.ArterialSpeedLevel = 4
    router.LocalRoadSpeedLevel = 4
    
    ret = router.DistanceTable(opts)
    
    # Get the name of the output table file 
    distance_table = next((v for k, v in ret if k == "OutFileName"), None)
    
    dk_table = dk.CreateGisdkObject("gis_ui", "Table", {"FileName":distance_table})
    dk_table.View()
 

# Initialize GISDK session
dk = caliperpy.Maptitude.connect()
pro = dk.Getprogram()
print(pro)
outfile = tempfile.mktemp(suffix=".bin")

# Select a few towns
print ("Selecting Towns")
dk.SetLayer ("City/Town")
query_string = "Select * where name = 'Cincinnati OH'  or name = 'Colerain OH' or name = 'Amberley OH' or  name = 'Dillionvale OH'"
dk.SelectByQuery("My Towns", "Several", query_string)

# Set up distance table options
opts = {
        "Origins": "My Towns",
        "Destinations": "My Towns", 
        "IsEuclid": "False",
        "OutputFileName": outfile
        }

table_file = compute_distance_table(dk, opts)
caliperpy.Maptitude.disconnect()