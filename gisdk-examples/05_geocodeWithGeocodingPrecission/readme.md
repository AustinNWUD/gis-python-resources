## Example: Geocode with geocoding precission

  This example shows how to output the geocoding precision.
## Before running:

#      Open the ParcelInfo.bin table, located in the Tutorial folder "C:\Users\[USERNAME]\Documents\Caliper\Maptitude 2025\Tutorial\ParcelInfo.bin"

---

## Inputs

- None.   
---

## Outputs

- A layer of points geocoded using different precission levels, with a field inidcatign the geocoding precission obtained for each record

---

## How to Run
1. Open Maptitude manually
2. Open a new map of the Unites States  *(File > New Workspace > New map of the United States)*.  For running the script on a country other than the US, you would need to replace the hardwired town names accordingly.

```bash
python geocode_with_geocoding_precission
