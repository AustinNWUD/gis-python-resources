# This Python code creates geocoded layers for all three methods

# Before running:

#      Open the ParcelInfo.bin table, located in the Tutorial folder 
#            "C:\Users\[USERNAME]\Documents\Caliper\Maptitude 2025\Tutorial\ParcelInfo.bin"

import caliperpy

def geocode_postal_code(dk):
    try:    
        # Fetch the view and create geocoder object
        view = dk.GetView()
        geo = dk.CreateGisdkObject("gis_ui", "Data.Geocoder")
        geo.SetRegion()

        # Define geocoding options
        opts = {
            "new_layer_name": "Parcels - Centered",
            "out_db": "c:\\temp\\Centered.dbd"
        }

        # First attempt: method "POSTAL_CODE|1"
        result_data = dict(geo.LocateView("POSTAL_CODE|1", view + "|", view + ".ID", [view + ".ZIP"], opts))
        views = dk.GetViews(None)

        # Second attempt: method "POSTAL_CODE|2"
        
        opts["out_db"] = "c:\\temp\\Scattered.dbd"
        opts["new_layer_name"] = "Parcels - Scattered evenly"
        geo.LocateView("POSTAL_CODE|2", view + "|", view + ".ID", [view + ".ZIP"], opts)
       
        # Third attempt: method "POSTAL_CODE|3"
        opts["out_db"] = "c:\\temp\\Scattered Within 1 mile.dbd"
        opts["new_layer_name"] = "Parcels - Scattered within 1 mile"
        geo.LocateView("POSTAL_CODE|3", view + "|", view + ".ID", [view + ".ZIP"], opts)

        return result_data
    except Exception as e:
        print("Error during iterative geocoding:", e)
    
# Example usage:
if __name__ == "__main__":
    dk = caliperpy.Maptitude.connect()
    geocode_postal_code(dk)
    caliperpy.Maptitude.disconnect()