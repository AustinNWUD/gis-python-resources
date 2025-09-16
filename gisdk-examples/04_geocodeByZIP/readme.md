# Example: Routing

The USA Country Data Catalog installed with Maptitude provides three methods for locating data by ZIP code in Maptitude:

- Centered at the ZIP Code Point

- Scattered Evenly inside the ZIP Code Area

- Scattered within 1 Mile of the ZIP Code

You can program this geocoding process by specifying which method to use in the Data.GeocoderLocateView macro. The method is selected by passing a first argument that defines the method to be executed.


To call a specific method, use:

result = geocoder.LocateView("method_name|method_number",...)
The method_name is always POSTAL_CODE.

The method_number can be 1, 2, or 3, corresponding to the three methods listed above.

If no method_number is specified, Maptitude defaults to method 1 (Centered at the ZIP Code Point).

The Python code creates a geocoded layer with poinns scattered within the ZIP codes.  
 
---

## Inputs

- None.  The script generates five stop points.

---

## Outputs

- A route report file in Excel format
- A map with a route connecting all points

---

## How to Run
1. Open Maptitude manually
2. Open a new map of the Unites States  (File > New Workspace > New map of the United States).  For running the scripot on a country other than the US, you would need to repalce the harwired coordinates accordingly.

```bash
python routing.py
