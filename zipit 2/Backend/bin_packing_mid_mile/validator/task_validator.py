from route_planner.utils import logging

import pandas as pd
import numpy as np

from route_planner.kohler.bin_packing_mid_mile.validator.base_validator import BaseValidator
from route_planner.utils.utils import generate_request_id

logger = logging.getLogger(__name__)


class TaskValidator(BaseValidator):
    SHEET = "Task"
    VALID_HEADER = ['Shipment ID*', 'From Node (Location Name)*', 'From Latitude', 'From Longitude', 'From City*',
                    'Consignee Name', 'RDC/Consignee (Location Name)*', 'To Latitude', 'To Longitude', 'To City*',
                    'Load (kg)', 'Volume (cbm)', 'Priority', 'Material Code*', 'Material Count*', 'Placement Date & Time*', 'SLA*']

    OUT_HEADER = ['task_id', 'from_location', 'from_latitude', 'from_longitude', 'from_city', 'consignee_name',
                  'to_location', 'to_latitude', 'to_longitude', 'to_city', 'load', 'volume', 'priority', 'mat_code', 'count', 'order_date_time', 'sla']

    IN_TO_OUT_HEADER_MAP = {'Shipment ID*': 'task_id',
                            'From Node (Location Name)*': 'from_location',
                            'From Latitude': 'from_latitude',
                            'From Longitude': 'from_longitude',
                            'Consignee Name': 'consignee_name',
                            'RDC/Consignee (Location Name)*': 'to_location',
                            'To Latitude': 'to_latitude',
                            'To Longitude': 'to_longitude',
                            'Load (kg)': 'load',
                            'Volume (cbm)': 'volume',
                            'From City*': 'from_city',
                            'To City*': 'to_city',
                            'Priority': 'priority',
                            'Material Code*': 'mat_code',
                            'Material Count*': 'count',
                            'Placement Date & Time*': 'order_date_time',
                            'SLA*': 'sla'
                            }

    MANDATORY_COLS = ['Shipment ID*', 'From Node (Location Name)*', 'RDC/Consignee (Location Name)*',
                      'From City*', 'To City*', 'Material Code*', 'Material Count*', 'Placement Date & Time*', 'SLA*']

    NUMBER_TYPE_COLS = ['Load (kg)', 'Volume (cbm)', 'Material Count*', 'SLA*']

    STRING_TYPE_COLS = ['Shipment ID*', 'From Node (Location Name)*', 'From City*', 'To City*',
                        'RDC/Consignee (Location Name)*', 'Material Code*']

    DATETIME_TYPE_COLS = ['Placement Date & Time*']

    TYPE_MAP_COLS = {'From Node (Location Name)*': BaseValidator.string_conversion,
                     'RDC/Consignee (Location Name)*': BaseValidator.string_conversion}


    def __init__(self, validator):
        self.problems = []
        logger.info(f"::: Validate Task :::")
        self.df = pd.read_excel(validator.file, self.SHEET, converters=self.TYPE_MAP_COLS)
        self.new_df = pd.DataFrame(index=self.df.index)
        self.header = self.df.columns.to_list()
        self.rid = validator.rid
        self.sku_exclusion_list = list()
        #self.UOM_TYPE = str()

    def sanitize(self):
        self.value_changes()

        self.new_df = pd.concat((self.df, self.new_df), axis=1)
        self.new_df.rename(columns=self.IN_TO_OUT_HEADER_MAP, inplace=True)
        self.new_df = self.new_df[self.OUT_HEADER]
        logger.info(f"({self.SHEET} sanitize) columns: {self.new_df.columns.to_list()}")

    def is_all_row_same(self, col):
        if not (self.df[col] == self.df[col][0]).all():
            indexes = self.df[col] != self.df[col][0]
            message = f"{col} should be same through out"
            rows = self.df[indexes].to_dict(orient="records")
            self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.SHEET))

    def valid_to_and_from_coordinate(self):
        from_lat = self.df['From Latitude'].copy()
        from_long = self.df['From Longitude'].copy()
        to_lat = self.df['To Latitude'].copy()
        to_long = self.df['To Longitude'].copy()
        lat_bool = to_lat == from_lat
        long_bool = to_long == from_long
        if (lat_bool & long_bool).any():
            indexes = self.df[(lat_bool & long_bool)].index.to_list()
            rows = self.df.loc[indexes].to_dict(orient='records')
            message = f"To Coordinates cannot be similar to From Coordinates"
            self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.SHEET))

    def lat_long_validation(self):
        if not self.df['From Latitude'].replace(r'^\s*$', np.nan, regex=True).isna().all() \
                and not self.df['From Longitude'].replace(r'^\s*$', np.nan, regex=True).isna().all():

            # latitude & longitude validation
            BaseValidator.validate_coordinate(self, self.df['From Latitude'], type='latitude')
            BaseValidator.validate_coordinate(self, self.df['From Longitude'], type='longitude')
            # 'From Location*', 'From Latitude*', 'From Longitude*' should consist all same entries
            validate_all_same_cols = ['From Node (Location Name)*', 'From Latitude', 'From Longitude']
            for col in validate_all_same_cols:
                self.is_all_row_same(col)

            if not self.df['To Latitude'].replace(r'^\s*$', np.nan, regex=True).isna().all() \
                    and not self.df['To Longitude'].replace(r'^\s*$', np.nan, regex=True).isna().all():
                # if not self.df['To Latitude'].isnull().values.any() and not self.df['To Longitude'].isnull().values.any():
                BaseValidator.validate_coordinate(self, self.df['To Latitude'], type='latitude')
                BaseValidator.validate_coordinate(self, self.df['To Longitude'], type='longitude')
                self.valid_to_and_from_coordinate()
            else:
                lat_indexes = self.df[self.df['To Latitude'].isnull()].index.tolist()
                long_indexes = self.df[self.df['To Longitude'].isnull()].index.tolist()
                indexes = lat_indexes + long_indexes
                rows = self.df.loc[indexes].to_dict(orient='records')
                message = "Found missing values in to latitude and longitude, " \
                          "if you require location service, remove from latitude longitude."
                self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.SHEET))

        elif self.df['From Latitude'].replace(r'^\s*$', np.nan, regex=True).isna().all() \
                and self.df['From Longitude'].replace(r'^\s*$', np.nan, regex=True).isna().all():
            if not self.df['To Latitude'].replace(r'^\s*$', np.nan, regex=True).isna().all() \
                    and not self.df['To Longitude'].replace(r'^\s*$', np.nan, regex=True).isna().all():
                lat_indexes = self.df[self.df['From Latitude'].isnull()].index.tolist()
                long_indexes = self.df[self.df['From Longitude'].isnull()].index.tolist()
                indexes = lat_indexes + long_indexes
                rows = self.df.loc[indexes].to_dict(orient='records')
                message = "Found missing values in from latitude and longitude, " \
                          "Both from/to latitude longitude should be empty to use location service."
                self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.SHEET))

    def load_volume_validation(self):
        if self.df['Load (kg)'].replace(r'^\s*$', np.nan, regex=True).isna().all() \
             and self.df['Volume (cbm)'].replace(r'^\s*$', np.nan, regex=True).isna().all():
            message = "Either Load or Volume is required."
            self.problems.extend(BaseValidator.add_problem([-1], dict(), message, self.SHEET))


    def type_validator(self):
        BaseValidator.string_type_validator(self, self.STRING_TYPE_COLS)

        BaseValidator.number_type_validator(self, self.NUMBER_TYPE_COLS)
        BaseValidator.datetime_type_validator(self, self.DATETIME_TYPE_COLS)

        BaseValidator.positive_number_validator(self, ['Load (kg)'])

        BaseValidator.greater_than_zero_validation(self, ['Load (kg)', 'Volume (cbm)', 'SLA*'])

        BaseValidator.type_cast_to_str(self)

        self.lat_long_validation()

        self.load_volume_validation()

        # BaseValidator.check_priority_value(self, 'Priority')

    def validate_header(self):
        return BaseValidator.validate_header(self)

    def process(self):
        self.type_validator()


    @staticmethod
    def generate_uuid():
        return generate_request_id()

    def value_changes(self):
        # For Case Insensitive
        self.df['From City*'] = self.df['From City*'].str.lower()
        self.df['To City*'] = self.df['To City*'].str.lower()

        # order_id
        self.df['order_id'] = self.df.index

        self.df['Load (kg)'] = self.df['Load (kg)'].fillna(0)
        self.df['Volume (cbm)'] = self.df['Volume (cbm)'].fillna(0)

