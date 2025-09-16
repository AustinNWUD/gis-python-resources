# Example: Routing

This example demonstrates how to **optimize a route along given points** using the **Caliper Maptitude GISDK** via the `caliperpy` Python API.  
 
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
2. Open a new map of the Unites States  (File > New Workspace > New map of the United States).  For running the scripot on a country other than the US, you would need to replace the hardwired coordinates accordingly.

```bash
python routing.py
