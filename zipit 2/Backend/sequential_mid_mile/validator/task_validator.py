from route_planner.exceptions.exceptions import ReverseGeocodingException
from route_planner.utils import logging

import pandas as pd
import numpy as np
import uuid

from route_planner.kohler.sequential_mid_mile.validator.base_validator import BaseValidator
from route_planner.utils.utils import generate_request_id, HereAPI

logger = logging.getLogger(__name__)


class TaskValidator(BaseValidator):
    SHEET = "Task"
    FILE_ROWS_LIMIT = 500
    VALID_HEADER = ['Shipment ID*', 'From Node (Location Name)*', 'From Latitude', 'From Longitude', 'From City*',
                    'Consignee Name', 'RDC/Consignee (Location Name)*', 'To Latitude', 'To Longitude', 'To City*',
                    'Load (kg)', 'Volume (cbm)', 'Priority', 'Material Code*', 'Material Count*',
                    'Placement Date & Time', 'SLA (Hours)']

    OUT_HEADER = ['task_id', 'from_location', 'from_latitude', 'from_longitude', 'from_city', 'consignee_name',
                  'to_location', 'to_latitude', 'to_longitude', 'to_city', 'load', 'volume', 'working_window_from',
                  'working_window_to', 'priority', 'mat_code', 'count', 'order_date_time', 'sla', 'request_id']

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
                            'Placement Date & Time': 'order_date_time',
                            'SLA (Hours)': 'sla'
                            }

    MANDATORY_COLS = ['Shipment ID*', 'From Node (Location Name)*', 'RDC/Consignee (Location Name)*',
                      'From City*', 'To City*', 'Material Code*', 'Material Count*']

    NUMBER_TYPE_COLS = ['Load (kg)', 'Volume (cbm)', 'Material Count*', 'SLA (Hours)', 'From Latitude', 'From Longitude',
                        'To Latitude', 'To Longitude']

    STRING_TYPE_COLS = ['Shipment ID*', 'From Node (Location Name)*', 'From City*', 'To City*',
                        'RDC/Consignee (Location Name)*', 'Material Code*']

    DATETIME_TYPE_COLS = ['Placement Date & Time']

    TYPE_MAP_COLS = {'From Node (Location Name)*': BaseValidator.string_conversion,
                     'RDC/Consignee (Location Name)*': BaseValidator.string_conversion,
                     'Consignee Name': BaseValidator.string_conversion}

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
        self.default_values()
        self.value_changes()
        # fill lat/long values by HERE API
        self.fill_coordinate_by_location(
            location_col=self.df['From Node (Location Name)*'],
            latitude_col=self.df['From Latitude'],
            longitude_col=self.df['From Longitude']
        )
        self.fill_coordinate_by_location(
            location_col=self.df['RDC/Consignee (Location Name)*'],
            latitude_col=self.df['To Latitude'],
            longitude_col=self.df['To Longitude']
        )
        self.valid_to_and_from_coordinate()

        self.new_df = pd.concat((self.df, self.new_df), axis=1)
        self.new_df.rename(columns=self.IN_TO_OUT_HEADER_MAP, inplace=True)
        self.new_df = self.new_df[self.OUT_HEADER]
        logger.info(f"({self.SHEET} sanitize) columns: {self.new_df.columns.to_list()}")

    def is_all_row_same(self, col) -> bool:
        if self.df[col].isnull().all():
            return True
        invalid_bool = self.df[col] != self.df[col][0]
        if invalid_bool.any():
            message = f"{col} should be same through out"
            indexes = self.df[col][invalid_bool].index.to_list()
            rows = {idx+2: self.df.loc[idx].to_dict() for idx in indexes}
            self.problems.extend(BaseValidator.add_problem([-1], rows, message, self.SHEET))
            return False
        return True

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

        validate_all_same_cols = ['From Node (Location Name)*', 'From Latitude', 'From Longitude']
        from_location_similar_bool = all([self.is_all_row_same(col) for col in validate_all_same_cols])

        # latitude & longitude range validation
        if from_location_similar_bool:
            BaseValidator.validate_coordinate(self, self.df['From Latitude'], type='latitude', drop_null=True)
            BaseValidator.validate_coordinate(self, self.df['From Longitude'], type='longitude', drop_null=True)

        BaseValidator.validate_coordinate(self, self.df['To Latitude'], type='latitude', drop_null=True)
        BaseValidator.validate_coordinate(self, self.df['To Longitude'], type='longitude', drop_null=True)

        # both latitude/longitude should be provided
        if from_location_similar_bool:
            self.validate_both_latitude_longitude_exists(
                latitude_col=self.df['From Latitude'],
                longitude_col=self.df['From Longitude']
            )
        self.validate_both_latitude_longitude_exists(
            latitude_col=self.df['To Latitude'],
            longitude_col=self.df['To Longitude']
        )

        #  from_location/to_location should be valid pincode if respective coordinates not given
        if from_location_similar_bool:
            self.validate_location_for_null_coordinates(
                location_col=self.df['From Node (Location Name)*'],
                latitude_col=self.df['From Latitude'],
                longitude_col=self.df['From Longitude']
            )
        self.validate_location_for_null_coordinates(
            location_col=self.df['RDC/Consignee (Location Name)*'],
            latitude_col=self.df['To Latitude'],
            longitude_col=self.df['To Longitude']
        )

        self.valid_to_and_from_coordinate()

    def load_volume_validation(self):
        # print(self.df['Load (kg)'].replace(r'^\s*$', np.nan, regex=True).isna().all())
        # print(self.df['Volume (cbm)'].replace(r'^\s*$', np.nan, regex=True).isna().all())
        if self.df['Load (kg)'].replace(r'^\s*$', np.nan, regex=True).isna().all() \
             and self.df['Volume (cbm)'].replace(r'^\s*$', np.nan, regex=True).isna().all():
            message = "Either Load or Volume is required."
            self.problems.extend(BaseValidator.add_problem([-1], dict(), message, self.SHEET))

    def row_load_volume_validation(self):
        col1 = self.df[self.df['Load (kg)'].replace(r'^\s*$', np.nan, regex=True).isna()]
        col2 = self.df[self.df['Volume (cbm)'].replace(r'^\s*$', np.nan, regex=True).isna()]
        index1 = col1.index.tolist()
        # print(index1)

        index2 = col2.index.tolist()
        # print(index2)
        common_row = list(set(index1).intersection(index2))
        rows = self.df.loc[common_row].to_dict(orient='records')
        # print(common_row)
        if common_row:
            message = f"Either Load or volume should be present for these rows"
            self.problems.extend(BaseValidator.add_problem(common_row, rows, message, self.SHEET))

    def type_validator(self):
        BaseValidator.check_white_spaces(self)

        BaseValidator.string_type_validator(self, self.STRING_TYPE_COLS)

        BaseValidator.number_type_validator(self, self.NUMBER_TYPE_COLS)

        BaseValidator.positive_number_validator(self, ['Load (kg)', 'SLA (Hours)'])

        BaseValidator.greater_than_zero_validation(self, ['Load (kg)', 'Volume (cbm)', 'SLA (Hours)'])

        # Convert Float pincodes (excel parsing issue) to integer str 122001.0 to 122001
        location_columns = ['From Node (Location Name)*', 'RDC/Consignee (Location Name)*']
        for location_col in location_columns:
            numeric_mask = self.df[location_col].str.replace('.', '')
            numeric_mask = numeric_mask.str.isdecimal()
            self.df[location_col][numeric_mask] = pd.to_numeric(self.df[location_col][numeric_mask], errors='coerce').fillna(self.df[location_col]).astype(int).astype(str)

        BaseValidator.type_cast_to_str(self)

        self.lat_long_validation()

        self.load_volume_validation()

        self.row_load_volume_validation()

        BaseValidator.datetime_type_validator(self)

        # BaseValidator.check_priority_value(self, 'Priority')

    def validate_header(self):
        return BaseValidator.validate_header(self)

    def process(self):
        self.type_validator()

        BaseValidator.validate_alpha_count(self, ['From City*', 'To City*'])

    @staticmethod
    def generate_uuid():
        return generate_request_id()

    def default_values(self):
        self.df.fillna({'Consignee Name': ' '}, inplace=True)

    def value_changes(self):
        # For Case Insensitive
        self.df['From City*'] = self.df['From City*'].str.lower()
        self.df['To City*'] = self.df['To City*'].str.lower()

        # order_id
        self.df['order_id'] = self.df.index

        self.df['Load (kg)'] = self.df['Load (kg)'].fillna(0)
        self.df['Volume (cbm)'] = self.df['Volume (cbm)'].fillna(0)

        # SLA to working windows for solver
        if not self.df['SLA (Hours)'].dropna().empty:
            if 'SLA (Hours)' in self.header:
                col = self.df['SLA (Hours)'] * 60
                self.new_df['working_window_from'] = 0
                self.new_df['working_window_to'] = col
                self.new_df['working_window_from'] = self.new_df['working_window_from'].astype('int64')
        else:
            self.new_df['working_window_from'] = np.nan
            self.new_df['working_window_to'] = np.nan

        self.df['request_id'] = self.df.apply(lambda _: str(uuid.uuid4()).replace('-', ''), axis=1)

    def validate_both_latitude_longitude_exists(self, latitude_col, longitude_col):
        invalid_bool = np.logical_xor(latitude_col.isna(), longitude_col.isna())
        if invalid_bool.any():
            indexes = self.df[invalid_bool].index.tolist()
            message = f"Provide both {latitude_col.name} and {longitude_col.name}, or leave both fields empty."
            rows = self.df.loc[indexes].to_dict(orient="records")
            self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.SHEET))

    def validate_location_for_null_coordinates(self, location_col, latitude_col, longitude_col):
        invalid_coordinate_bool = np.logical_and(latitude_col.isna(), longitude_col.isna())
        _location_col = location_col[invalid_coordinate_bool]

        # Location cannot be Null if Coordinates not provided
        if _location_col.isna().any():
            indexes = _location_col[_location_col.isna()].index.tolist()
            message = f"{location_col.name} cannot be empty if coordinates ({latitude_col.name}, {longitude_col.name}) not provided"
            rows = self.df.loc[indexes].to_dict(orient="records")
            self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.SHEET))

        # validate location is valid pincode
        pattern = r'^[1-9][0-9]{5}$'
        _location_col = _location_col.dropna()
        mask = _location_col.str.match(pattern)
        _location_col[~mask] = np.nan

        if _location_col.isna().any():
            indexes = _location_col[_location_col.isna()].index.tolist()
            message = f"{location_col.name} should be a valid pincode"
            rows = self.df.loc[indexes].to_dict(orient="records")
            self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.SHEET))


    @staticmethod
    def get_lat_long(pincode):
        try:
            address = f"{pincode}, India"
            here_obj = HereAPI()
            coordinate = here_obj.get_coordinate_from_location(address)
            return coordinate.get('latitude'), coordinate.get('longitude')
        except ReverseGeocodingException as ex:
            return np.nan, np.nan

    def fill_coordinate_by_location(self, location_col, latitude_col, longitude_col):

        valid_bool = np.logical_and(latitude_col.isna(), longitude_col.isna())
        if not valid_bool.any():
            return

        lat_long_df = self.df.loc[valid_bool, location_col.name].apply(self.get_lat_long).apply(pd.Series)
        self.df.loc[valid_bool, latitude_col.name] = lat_long_df[0]
        self.df.loc[valid_bool, longitude_col.name] = lat_long_df[1]

        invalid_bool = np.logical_and(self.df[latitude_col.name].isna(), self.df[longitude_col.name].isna())

        if invalid_bool.any():
            index_bool = invalid_bool == True
            indexes = self.df[index_bool].index.tolist()
            message = f"Unable to find coordinates for  {longitude_col.name}"
            rows = self.df.loc[indexes].to_dict(orient="records")
            self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.SHEET))
