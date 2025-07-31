from route_planner.utils import logging

import pandas as pd
import uuid

from route_planner.constants.app_constants import AVERAGE_VEHICLE_SPEED
from route_planner.kohler.sequential_mid_mile.validator.base_validator import BaseValidator

logger = logging.getLogger(__name__)


class VehiclesValidator(BaseValidator):
    SHEET = "Vehicles"
    FILE_ROWS_LIMIT = 800

    VALID_HEADER = ['Contract ID',
                    'Lane Code',
                    'Transporter Name',
                    'Vehicle Type*',
                    'Weight Capacity (kg)',
                    'Volume Capacity (cbm)',
                    'Fixed Charges*',
                    'Per Touch Point Charges',
                    'From City*',
                    'To City*',
                    'Weight Utilisation % Lower Bound',
                    'Volume Utilisation % Lower Bound',
                    'Max Allowed Lane Deviation (km)',
                    'Max Touch Points Allowed',
                    'No. of vehicles']

    OUT_HEADER = ['contract_id', 'lane_code', 'transporter_name', 'vehicle_type_name', 'capacity',
                  'volumetric_capacity', 'fixed_charges', 'tp_cost',
                  'from_city', 'to_city', 'utilisation_lb', 'volume_utilisation_lb',
                  'tp_detour', 'tp_limit', 'num_vehicles', 'vehicle_index', 'average_speed',
                  'request_id']

    IN_TO_OUT_HEADER_MAP = {'Contract ID': 'contract_id',
                            'Lane Code': 'lane_code',
                            'Transporter Name': 'transporter_name',
                            'Vehicle Type*': 'vehicle_type_name',
                            'Weight Capacity (kg)': 'capacity',
                            'Volume Capacity (cbm)': 'volumetric_capacity',
                            'Fixed Charges*': 'fixed_charges',
                            'Per Touch Point Charges': 'tp_cost',
                            'From City*': 'from_city',
                            'To City*': 'to_city',
                            'Weight Utilisation % Lower Bound': 'utilisation_lb',
                            'Volume Utilisation % Lower Bound': 'volume_utilisation_lb',
                            'Max Allowed Lane Deviation (km)': 'tp_detour',
                            'Max Touch Points Allowed': 'tp_limit',
                            'No. of vehicles': 'num_vehicles',
                            }

    MANDATORY_COLUMNS = ['Vehicle Type*', 'Fixed Charges*', 'From City*', 'To City*']

    NUMBER_TYPE_COLUMNS = [
        'Weight Capacity (kg)',
        'Volume Capacity (cbm)',
        'Fixed Charges*',
        'Per Touch Point Charges',
        'Weight Utilisation % Lower Bound',
        'Volume Utilisation % Lower Bound',
        'Max Allowed Lane Deviation (km)',
        'Max Touch Points Allowed',
        'No. of vehicles']

    STRING_TYPE_COLS = ['Vehicle Type*', 'From City*', 'To City*']

    TYPE_MAP_COLS = {
        'Contract ID': BaseValidator.string_conversion,
        'Lane Code': BaseValidator.string_conversion,
        'Transporter Name': BaseValidator.string_conversion,
        'Vehicle Type*': BaseValidator.string_conversion,
        'From City*': BaseValidator.string_conversion,
        'To City*': BaseValidator.string_conversion,
    }

    def __init__(self, Validator):
        self.problems = list()
        logger.info(f"::: Validate Vehicles :::")
        self.df = pd.read_excel(Validator.file, self.SHEET)
        self.new_df = pd.DataFrame(index=self.df.index)
        self.header = self.df.columns.to_list()
        self.dedicated = False
        self.MANDATORY_COLS = self.MANDATORY_COLUMNS.copy()
        self.NUMBER_TYPE_COLS = self.NUMBER_TYPE_COLUMNS.copy()
        self.rid = Validator.rid
        #self.UOM_TYPE = str()

    def sanitize(self):
        self.default_values()
        self.value_changes()

        self.new_df = pd.concat((self.df, self.new_df), axis=1)
        self.new_df.rename(columns=self.IN_TO_OUT_HEADER_MAP, inplace=True)
        self.new_df = self.new_df[self.OUT_HEADER]

        logger.info(f"({self.SHEET} sanitize) columns: {self.new_df.columns.to_list()}")

    def type_validator(self):
        BaseValidator.check_white_spaces(self)
        BaseValidator.string_type_validator(self, self.STRING_TYPE_COLS)
        BaseValidator.number_type_validator(self, self.NUMBER_TYPE_COLS)
        BaseValidator.positive_number_validator(self, self.NUMBER_TYPE_COLS)
        BaseValidator.greater_than_zero_validation(self, ['Weight Capacity (kg)', 'Volume Capacity (cbm)'])
        BaseValidator.type_cast_to_str(self)

    def default_values(self):
        self.df.fillna({'Fixed Charges*': 0, 'Per Touch Point Charges': 0, 'Max Allowed Lane Deviation (km)': 0,
                        'Max Touch Points Allowed': 1, 'No. of vehicles': 100, 'Contract ID': ' ', 'Lane Code': ' '},
                       inplace=True)

    def validate_header(self):
        return BaseValidator.validate_header(self)

    def process(self):
        self.type_validator()

        self.from_to_validation()

        # 'Utilisation % Lower Bound' default 0 and range(0-100)
        self.utilization_validation('Weight Utilisation % Lower Bound')
        self.utilization_validation('Volume Utilisation % Lower Bound')

        BaseValidator.validate_alpha_count(self, ['From City*', 'To City*'])

    def from_to_validation(self):
        # value should exist in both To City and From City
        x = self.df['From City*'].copy().notnull()
        y = self.df['To City*'].copy().notnull()
        working_window_bool = x ^ y
        if working_window_bool.any():
            # for 01 and 10 case
            indexes = working_window_bool[working_window_bool == True].index.to_list()
            rows = self.df.loc[indexes].to_dict(orient="records")
            message = f"Invalid both 'From City' and 'To City' should be provided!"
            self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.SHEET))
            return False
        return True

    def utilization_validation(self, col_name):
        col = self.df[col_name].copy().copy().fillna(0)
        col_upper_bound = col > 100
        col_lower_bound = col < 0
        if (col_lower_bound | col_upper_bound).any():
            indexes = self.df[(col_lower_bound | col_upper_bound)].index.to_list()
            rows = self.df.loc[indexes].to_dict(orient='records')
            message = f"Field in {col_name} breaches range (0-100)"
            self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.SHEET))
            return False

        self.df[col_name] = self.df[col_name].fillna(0)
        return True

    def value_changes(self):
        # For Case Insensitive
        # f = self.df['From City'].copy().dropna().astype(str).str.lower()
        # t = self.df['To City'].copy().dropna().astype(str).str.lower()
        #
        # self.df['From City'].loc[f.index] = f
        # self.df['To City'].loc[t.index] = t

        self.df['Weight Capacity (kg)'] = self.df['Weight Capacity (kg)'].fillna(0)
        self.df['Volume Capacity (cbm)'] = self.df['Volume Capacity (cbm)'].fillna(0)

        self.df['From City*'] = self.df['From City*'].str.lower()
        self.df['To City*'] = self.df['To City*'].str.lower()

        self.new_df['dedicated'] = self.dedicated

        self.df['vehicle_index'] = self.df.index

        self.df['average_speed'] = AVERAGE_VEHICLE_SPEED

        BaseValidator.int_conversion('Max Touch Points Allowed', self.df)

        self.df['request_id'] = self.df.apply(lambda _: str(uuid.uuid4()).replace('-', ''), axis=1)

