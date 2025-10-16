import arcpy
import pandas as pd
from arcgis.features import GeoAccessor, GeoDaskSeriesAccessor
from datetime import datetime
import os

class FeatureClassUpdateChecker:
    def __init__(self,
                 previous_data : str,
                 new_data : str,
                 output_directory : str,
                 unique_id_field : str):
        
        current = datetime.now().strftime("%m%d%Y%H%M%S")
        out_db = f'review_{current}.gdb'
        out_file = f'reviewTable_{current}'
        
        self.out_table = os.path.join(output_directory, out_db, out_file)
        self.previous_data = pd.DataFrame.spatial.from_featureclass(previous_data)
        self.new_data = pd.DataFrame.spatial.from_featureclass(new_data)
        self.unique_id_field = unique_id_field
        self.out_flds = ['update_type', 'unique_id', 'description']
        
        arcpy.management.CreateFileGDB(output_directory, out_db)
        arcpy.management.CreateTable(f"{output_directory}\\{out_db}", out_file)
        
        #adds summary fields to created table to populate
        arcpy.management.AddField(self.out_table,
                          field_name = 'update_type',
                          field_type = 'TEXT')
        arcpy.management.AddField(self.out_table, 
                          field_name = 'unique_id',
                          field_type= 'TEXT')
        arcpy.management.AddField(self.out_table,
                          field_name = 'description',
                          field_type = 'TEXT',
                          field_length=8000)
        
    def check_fields(self, ignore_fields = []):
        
        
        previous_data = self.previous_data
        new_data = self.new_data
        
        
        prev_flds = [f for f in list(previous_data.columns) 
                     if f not in ignore_fields]
        new_flds = [f for f in list(new_data.columns) 
                    if f not in ignore_fields]
        
        legacy = [f for f in prev_flds if f not in new_flds]
        created = [f for f in new_flds if f not in prev_flds]
        
        self.authoritative_test_fields = [f for f in prev_flds 
                                          if f in new_flds]

        data_dump = []
        
        with arcpy.da.InsertCursor(self.out_table, self.out_flds) as cursor:
            if len(legacy) > 0:
                for l in legacy:
                    up = ['DELETED FIELD', l,
                            'Legacy field not present in new table']
                    cursor.insertRow(up)
                    data_dump.append(up)
            if len(created) > 0:
                for c in created:
                    up = ['ADDED FIELD', c,
                            'New field added to layer']
                    data_dump.append(up)
                        
            if len(data_dump) > 0:
                field_updates = data_dump
                total_ups = len(data_dump)
            else:
                field_updates = None
                total_ups = 0
                
            result = {
                'updates' : field_updates,
                'count' : total_ups,
                'written_to' : self.out_table
            }
            
            return result
    
    def check_features(self, ignore_ids = []):
        
        id_fld = self.unique_id_field
        prev_data = self.previous_data
        new_data = self.new_data
        
        
        old_ids = [f for f in list(prev_data[id_fld]) if f not in ignore_ids]
        new_ids = [f for f in list(new_data[id_fld]) if f not in ignore_ids]

        deleted = [f for f in old_ids if f not in new_ids]
        added = [f for f in new_ids if f not in old_ids]

        data_dump = []
        with arcpy.da.InsertCursor(self.out_table, self.out_flds) as cursor:
            if len(deleted) > 0:
                for d in deleted:
                    up = ['DELETED_FEATURE', d, 'Feature not in updated table']
                    cursor.insertRow(up)
                    data_dump.append(up)
                    
            if len(added) > 0:
                for a in added:
                    up =['ADDED_FEATURE', a, 'New feature in update table']
                    data_dump.append(up)
                    
        self.validated_ids = [i for i in old_ids if i in new_ids]
        valid_data = new_data.loc[new_data[id_fld].isin(self.validated_ids)]
        
        with arcpy.da.InsertCursor(self.out_table, self.out_flds) as cursor:
            for index, row in valid_data.iterrows():
                old_row = prev_data[self.authoritative_test_fields].loc[
                    prev_data[id_fld] == row[id_fld]].iloc[0]
                new_row = new_data[self.authoritative_test_fields].loc[
                    new_data[id_fld] == row[id_fld]].iloc[0]
                
                validate = dict(old_row == new_row)
                
                if False in validate.values():
                    up = ['UPDATED_FEATURE', row[id_fld], str(validate)]    
                    cursor.insertRow(up)
                    data_dump.append(up)
                    
        if len(data_dump) > 0:
            updates = data_dump
            count = len(data_dump)
        else:
            updates = None
            count = 0
            
        result = {
            'updates' : updates,
            'count' : count,
            'written_to' : self.out_table
        }
        
        return result
            