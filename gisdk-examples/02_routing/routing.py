# This example demonstrates how to **optimize a route along given points** using the **Caliper Maptitude GISDK** via the `caliperpy` Python API.  
# Before running:  Open a map of the USA.


import caliperpy

import ctypes

from ctypes.wintypes import HWND, LPWSTR, UINT

_user32 = ctypes.WinDLL('user32', use_last_error=True)

_MessageBoxW = _user32.MessageBoxW

_MessageBoxW.restype = UINT

_MessageBoxW.argtypes = (HWND, LPWSTR, LPWSTR, UINT)

MB_OK = 0

dk = caliperpy.Maptitude.connect()

p = dk.GetProgram()

try:

   dk.SetMapUnits("Miles")

   units = dk.GetMapUnits(None)

   p1 = {"Coordinate": dk.Coord(-71259994, 42298892), "StopDuration": 30,

           "StopName": "Babson College"}

   p2 = {"Coordinate": dk.Coord(-71250024, 42341178), "StopDuration": 30,

           "StopName": "Lasell College"}

   p3 = {"Coordinate": dk.Coord(-71187232, 42298633), "StopDuration": 30,

           "StopName": "Mount Ida College"}

   p4 = {"Coordinate": dk.Coord(-71197377, 42340143), "StopDuration": 30,

           "StopName": "Boston College"}

   p5 = {"Coordinate": dk.Coord(-71310381, 42298016), "StopDuration": 30,

           "StopName": "Wellesley College"}

   RoutingPoints = [p1, p2, p3, p4, p5]

   # Zoom the map to the route stops
   points = [p["Coordinate"] for p in RoutingPoints]

   scp = dk.GetArrayScope(points)

   scp.width = scp.width * 1.2  # Zoom out a bit more 

   dk.SetMapScope( None, scp)

   dk.RedrawMap(None)

   # Create the route
   router = dk.CreateGisdkObject("gis_ui", "Routing.Router")

   router.Minimize =  "Time"

   router.IncludeRestStops = True

   router.TimeBetweenRests = 5

   router.RestStopDuration = 20

   router.FuelPrice = 3.29

   # Modify speed factor by link type 1: slow, 4: Normal, 7: fast

   router.MajorHighwaySpeedLevel  = 4

   router.SecondaryHighwaySpeedLevel  = 4

   router.LocalHighwaySpeedLevel = 4

   router.ArterialSpeedLevel = 4

   router.LocalRoadSpeedLevel = 4

   spOpts =  {"Fix":"Last", "Loop": "False"}  # Modify the Fix and Loop options as needed

   path = router.Calculate(RoutingPoints, )

   if path is not None:

       time = path.Time

       dist = path.Distance
       path.DisplayPath()

       ret = router.CreateReport({"PathObj":path, "OpenReport":1,

           "FileName": dk.GetRandFileName("*.xlsx")})

       retOpts = dict(ret)

       if ( not retOpts is None ) and ( 'ErrorMessage' in retOpts ):

           _MessageBoxW(0, retOpts["ErrorMessage"], "Error", MB_OK)

       else:

            _MessageBoxW(0, "Path length: " + str(dist) + " : " + "Path Time: " + str(time), "Information", MB_OK)
                        

   else:

       raise Exception("Please open a map window first")
    caliperpy.Maptitude.disconnect()

  

except Exception as Error:

   _MessageBoxW(0, repr(error), "Information", MB_OK)
   caliperpy.Maptitude.disconnect()