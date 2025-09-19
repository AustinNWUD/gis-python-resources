#This example shows how to output the geocoding precision.

# Before running:

#      Open the ParcelInfo.bin table, located in the Tutorial folder "C:\Users\[USERNAME]\Documents\Caliper\Maptitude 2025\Tutorial\ParcelInfo.bin"

 

import caliperpy
 
def geocode_with_precision(dk):
    try:
        # Get the current view (this might return the current active table/view name)
        view = dk.GetView()
        # Optionally, retrieve field information from the view
        field_data = dk.GetFields(view, "All")  # Returns (fieldNames, fieldSpecs)
 
        geo = dk.CreateGisdkObject("gis_ui", "Data.Geocoder")
        geo.SetRegion()
        region_name = geo.GetRegionName()
        print(f"Geocoding view {view} in region {region_name}")
 
        # Get field specifications for the geocoding process
        id_field = dk.GetFieldFullSpec(view, "ID")
        address_field = dk.GetFieldFullSpec(view, "Address")
        postal_field = dk.GetFieldFullSpec(view, "ZIP")
        city_field = dk.GetFieldFullSpec(view, "City")
        state_field = dk.GetFieldFullSpec(view, "State")
 
        # Set geocoding options, including the best_match flag and method preferences
        opts = {
            "best_match": 1,  # Ensures the "Geocoding Precision" field is populated
            "try_methods": [1, 1, 1, 1, 1, 1],  # Methods ordered from most to least accurate
            "new_layer_name": view + " Layer",
            "out_db": "c:\\temp\\" + view + " Layer.dbd"
        }
 
        # Input field specifications for the best match geocoder:
        # Order: address, address2, city, state, postal_code
        input_field_specs = [address_field, None, city_field, state_field, postal_field]
 
        result = geo.LocateView("ADDRESS_WIZARD", view + "|", id_field, input_field_specs, opts)
        print("Geocoding with precision result:", result)

        return result
 
    except Exception as e:
        print("Error during geocoding with precision:", e)
 
# Example usage:
if __name__ == "__main__":
    dk = caliperpy.Maptitude.connect()
    geocode_with_precision(dk)