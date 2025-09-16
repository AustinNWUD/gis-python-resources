# Before running:
# 1. Open Maptitude manually
# 2. Open the ParcelInfo.bin table, located in the Tutorial folder:
#    "C:\Users\[USERNAME]\Documents\Caliper\Maptitude 2025\Tutorial\ParcelInfo.bin"

import caliperpy

def iterative_geocoding (dk):
    try:
        # Assume the correct view/table is open and current
        view = dk.GetView()
        out_folder = "c:\\temp\\"

        # Create a Data.Geocoder object and set the region
        geo = dk.CreateGisdkObject("gis_ui", "Data.Geocoder")
        geo.SetRegion()
        region_name = geo.GetRegionName()
        print(f"Locating view {view} in region {region_name}")

        # Get field specifications for the input view
        id_field = dk.GetFieldFullSpec(view, "ID")
        address_field = dk.GetFieldFullSpec(view, "Address")
        postal_field = dk.GetFieldFullSpec(view, "ZIP")
        city_field = dk.GetFieldFullSpec(view, "City")
        state_field = dk.GetFieldFullSpec(view, "State")

        # Define geocoding options
        opts = {
            "new_layer_name": view + "_Layer",
            "out_db": out_folder + view + "_Layer.dbd"
        }

        # First pass: geocode by ADDRESS method using address and postal code
        result = geo.LocateView("ADDRESS", view + "|", id_field, [address_field, None, postal_field], opts)
        result_dict = dict(result)
        layer_name = result_dict["LayerName"]
        
        # Update field specs for the newly created layer
        id_field = dk.GetFieldFullSpec(layer_name, "ID")
        postal_field = dk.GetFieldFullSpec(layer_name, "ZIP")
        city_field = dk.GetFieldFullSpec(layer_name, "City")
        state_field = dk.GetFieldFullSpec(layer_name, "State")

        # If some records were not found, try geocoding again by POSTALCODE
        not_found_set = result_dict.get("NotFoundSet")
        if not_found_set is not None and layer_name is not None:
            input_fields = [postal_field]
            result = geo.LocateView("POSTALCODE", view + "|", id_field, input_fields, opts)

        # For any records still not found, try geocoding by CITY
        not_found_set = result_dict.get("NotFoundSet")
        if not_found_set is not None and layer_name is not None:
            result = geo.LocateView("CITY", layer_name + "|" + not_found_set, id_field, [city_field, state_field], opts)

        print("Iterative geocoding result:", result)
        return result

    except Exception as e:
        print("Error during iterative geocoding:", e)

# Example usage:
if __name__ == "__main__":
    dk = caliperpy.Maptitude.connect()
    iterative_geocoding(dk)
    caliperpy.Maptitude.disconnect()
