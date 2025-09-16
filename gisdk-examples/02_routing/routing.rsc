// Generate a shortest path via 5 routing points and display on the current map window

Macro "routing" 

   on error do

       ShowMessage(GetLastError())

       return()

   end

   p1.Coordinate = Coord(-71259994, 42298892)

   p1.StopDuration = 30

   p1.StopName = "Babson College"

  

   p2.Coordinate = Coord(-71250024, 42341178)

   p2.StopName = "Lasell College"

   p2.StopDuration = 120

  

   p3.Coordinate = Coord(-71187232, 42298633)

   p3.StopDuration = 43

   p3.StopName = "Mount Ida College"

   p4.Coordinate = Coord(-71197377, 42340143)

   p4.StopName = "Boston College"

   p5.Coordinate = Coord(-71310381, 42298016)

   p5.StopName = "Wellesley College"

   RoutingPoints = {p1, p2, p3, p4, p5}

   router = CreateObject("Routing.Router")

      

   router.Minimize = "Time"

   router.IncludeRestStops = true

   router.TimeBetweenRests = 5

   router.RestStopDuration = 20

   router.FuelPrice = 3.29

   router.SingleThreaded = true

      

   // Modify speed factor by link type 1: slow, 4: Normal, 7: fast

   router.MajorHighwaySpeedLevel  = 4

   router.SecondaryHighwaySpeedLevel  = 4

   router.LocalHighwaySpeedLevel = 4

   router.ArterialSpeedLevel = 4

   router.LocalRoadSpeedLevel = 4

   spOpts = null

   spOpts.Fix = "First"

   spOpts.Loop = False

   path = router.Calculate(RoutingPoints, spOpts)

   if path.Error then

       Throw(router.ErrorMessage)

   // create a report

   ret = router.CreateReport({PathObj: path, OpenReport: true,

                             FileName: GetRandFileName("*.xlsx")})

   if ret.Error then Throw(ret.ErrorMessage)

   //    ret = router.ExportToExcel({FileName: GetTempFileName("*.xlsx"), OpenExcel: true})

   //    if ret.Error then Throw(ret.ErrorMessage)   

  // Display path on current map

   path.PathDisplayWidth = 60

      

   shared cc_Colors

   path.PathDisplayColor = cc_Colors.Green

   path.DisplayPath()

//    path.ClearPathDisplay()

 

