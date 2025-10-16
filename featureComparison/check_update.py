import arcpy
import pandas as pd
from arcgis.features import GeoAccessor, GeoDaskSeriesAccessor
from datetime import datetime
import os
import sys

sys.path.append(r"C:\Users\austi\Documents\gitResources\gis-python-resources\featureComparison\pyModule\lib")

from pyModule.lib.updated_layer import FeatureClassUpdateChecker

base = r"C:\Users\austi\Documents\gitResources\gis-python-resources\featureComparison"
prev_data = f"{base}\\example_data\\originalData.gdb\\polygons"
new_data = f"{base}\\example_data\\updatedData.gdb\\polygons"
output_directory = f"{base}\\outputs"
unique_fld = 'unique_id'


updateChecker = FeatureClassUpdateChecker(prev_data,
                                          new_data,
                                          output_directory,
                                          unique_fld)

flds = updateChecker.check_fields(ignore_fields = [])

print(f"{flds['updates']}\n{flds['count']}")

feats = updateChecker.check_features(ignore_ids=[])

print(f"{feats['updates']}\n{feats['count']}")