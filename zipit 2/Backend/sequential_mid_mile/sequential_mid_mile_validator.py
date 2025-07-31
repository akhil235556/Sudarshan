import copy
import json
import datetime

import numpy as np
import uuid
from route_planner.constants.app_constants import UploadJobsStatus, SolverStatus

from route_planner.utils import logging
import os

import pandas as pd

from memory_profiler import profile

from route_planner.constants.app_configuration import AppConfig
from route_planner.constants import app_constants
from route_planner.constants.requests_log_messages import RequestLogMessages
from route_planner.exceptions.exceptions import ValidationError, AppException, RequestTerminationException, SolverException
from route_planner.gcp.prima_storage_config import PrimaGoogleStorageConfiguration
from route_planner.services.upload_jobs import RequestsServices
from route_planner.kohler.sequential_mid_mile.validator.base_validator import BaseValidator
from route_planner.kohler.sequential_mid_mile.validator.task_validator import TaskValidator
from route_planner.kohler.sequential_mid_mile.validator.vehicles_validator import VehiclesValidator
from route_planner.kohler.sequential_mid_mile.sequential_mid_mile_configuration import SequentialMidMileSolverConfiguration
from route_planner.kohler.sequential_mid_mile.tsp_solver_configuration import TSPSolverConfiguration
from solvers.solver_v6.exporter import export_routes_details, export_aggregated_details, export_confusion_matrix_details, export_summary_info, export_vehicle_info, export_vehicles_data, export_indent_details
from solvers.solver_v6.utils import vehicle_multiplier
from route_planner.utils.utils import get_current_timestamp, pincode_to_latlong, remove_suffix
from solvers.solver_v5.solver import SolverV5
from solvers.solver_v6.solver import SolverVRP
from route_planner.services.requests_log import RequestLog
from route_planner.integrations.route_planning_service import RoutePlanningService

logger = logging.getLogger(__name__)


class SequentialMidMileUploadValidation(object):

    def __init__(self, file, rid, search_time_limit, partition=None, node=None, tenant=None):
        self.file = file
        self._rid = rid
        self.problems = list()
        self._VALID_VEHICLES_LIST = list()
        self.extra_values_list = list()
        self.search_time_limit = search_time_limit
        self.partition = partition
        self.node = node
        self.tenant = tenant
        self.base_validator = BaseValidator(self.file, self._rid)
        self.task_validator = TaskValidator(self.base_validator)
        self.vehicles_validator = VehiclesValidator(self.base_validator)
        self.timestamp = get_current_timestamp()

    def _valid_vehicle_type(self, value):
        if isinstance(value, str):
            vehicles_list = value.lower().split(',')
            return set(vehicles_list).issubset(set(self._VALID_VEHICLES_LIST))
        return False

    def _get_actual_vehicle_case(self, invalid_vehicles, value) -> list:
        vehicle_actual_case = value.split(",")
        vehicle_lower_case = value.lower().split(",")

        invalid_indexes = [vehicle_lower_case.index(veh) for veh in invalid_vehicles]
        valid_vehicles = [vehicle_actual_case[idx] for idx in invalid_indexes]
        return valid_vehicles

    def _filter_error(self, value):
        """
        serviceable_vehicles_validation helper method.

        Filter valu

        Parameters:
        ----------
         value : str
            Comma separated string
        """
        if isinstance(value, str):
            vehicles_list = value.lower().split(',')
            invalid_vehicles = list(set(vehicles_list) - (set(self._VALID_VEHICLES_LIST)))
            if invalid_vehicles:
                return ",".join(self._get_actual_vehicle_case(invalid_vehicles, value))
        return value

    def demand_with_capacity_validation(self):
        task = self.task_validator.df.copy()
        vehicles = self.vehicles_validator.df.copy()

        if not task['Load (kg)'].replace(r'^\s*$', np.nan, regex=True).isna().all():
            if vehicles['Weight Capacity (kg)'].replace(r'^\s*$', np.nan, regex=True).isna().all():
                message = "If load is present in Task sheet, then vehicle weight capacity is required."
                self.problems.extend(BaseValidator.add_problem([-1], dict(), message, self.task_validator.SHEET))
            elif vehicles[vehicles['Weight Capacity (kg)'].isnull()].index.tolist():
                indexes = vehicles[vehicles['Weight Capacity (kg)'].isnull()].index.tolist()
                rows = vehicles.loc[indexes].to_dict(orient='records')
                message = "If load is present in Task sheet, then vehicle weight capacity is required."
                self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.vehicles_validator.SHEET))

        if not task['Volume (cbm)'].replace(r'^\s*$', np.nan, regex=True).isna().all():
            if vehicles['Volume Capacity (cbm)'].replace(r'^\s*$', np.nan, regex=True).isna().all():
                message = "If volume is present in Task sheet, then vehicle volume capacity is required."
                self.problems.extend(BaseValidator.add_problem([-1], dict(), message, self.task_validator.SHEET))
            elif vehicles[vehicles['Volume Capacity (cbm)'].isnull()].index.tolist():
                indexes = vehicles[vehicles['Volume Capacity (cbm)'].isnull()].index.tolist()
                rows = vehicles.loc[indexes].to_dict(orient='records')
                message = "If volume is present in Task sheet, then vehicle volume capacity is required."
                self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.vehicles_validator.SHEET))

    @staticmethod
    def _sanitize_serviceable_vehicles(value, valid_vehicle_map):
        vehicles_list = value.lower().split(',')
        vehicles_list = [valid_vehicle_map.get(v) for v in vehicles_list]
        return ",".join(vehicles_list)

    def serviceable_vehicles_validation(self):
        """
        Validate Serviceable Vehicles col. in Task by Valid Vehicles
        """
        self._VALID_VEHICLES_LIST = self.vehicles_validator.df['Vehicle Type*'].copy().str.lower().to_list()
        error = False
        col = self.task_validator.df['Serviceable Vehicles'].dropna().copy()
        valid_rows = col.map(lambda x: self._valid_vehicle_type(x))
        if not valid_rows.all():
            indexes = valid_rows[valid_rows == False].index.to_list()
            filtered_rows = self.task_validator.df.loc[indexes].copy()
            filtered_rows['Serviceable Vehicles'] = filtered_rows['Serviceable Vehicles'].map(lambda x: self._filter_error(x))
            rows = filtered_rows.to_dict(orient='records')
            message = f"Vehicle in Serviceable Vehicles not exists in Vehicle Sheet"
            self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.task_validator.SHEET))
            error = True

        col = self.task_validator.new_df['serviceable_vehicles'].copy().dropna()

        if not error:
            indexes = col.index.to_list()
            _VALID_VEHICLES = self.vehicles_validator.df['Vehicle Type*'].copy().to_list()
            _valid_vehicle_map = {str(v).lower(): v for v in _VALID_VEHICLES}
            # col = col.map(lambda x: MultiProductUploadValidation._sanitize_serviceable_vehicles(x, _valid_vehicle_map))
            # self.task_validator.new_df['serviceable_vehicles'].loc[indexes] = col.values

    def raise_problems(self):
        if self.problems:
            raise ValidationError(problems=self.problems)

    def validate_file_length(self):
        valid = True
        if not self.base_validator.validate_file_length(self.task_validator):
            self.problems.extend(self.task_validator.problems)
            valid = False

        if not self.base_validator.validate_file_length(self.vehicles_validator):
            self.problems.extend(self.vehicles_validator.problems)
            valid = False

        return valid

    def validate_headers(self):
        valid = True
        if not self.task_validator.validate_header():
            self.problems.extend(self.task_validator.problems)
            valid = False

        if not self.vehicles_validator.validate_header():
            self.problems.extend(self.vehicles_validator.problems)
            valid = False

        return valid

    def validate_mandatory_fields(self):
        valid = True
        if not self.base_validator.check_MandatoryField(self.task_validator):
            self.problems.extend(self.task_validator.problems)
            valid = False

        if not self.base_validator.check_MandatoryField(self.vehicles_validator):
            self.problems.extend(self.vehicles_validator.problems)
            valid = False

        return valid

    def validate_from_to_city_vehicles(self):
        """
        Validate if From City & To City vehicle is present
        for all task
        """
        self.task_validator.df['From City*'] = self.task_validator.df['From City*'].str.lower()
        self.task_validator.df['To City*'] = self.task_validator.df['To City*'].str.lower()
        self.vehicles_validator.df['From City*'] = self.vehicles_validator.df['From City*'].str.lower()
        self.vehicles_validator.df['To City*'] = self.vehicles_validator.df['To City*'].str.lower()

        task = self.task_validator.df.copy()
        t = task[['From City*', 'To City*']].copy().apply(tuple, axis=1).to_list()
        v = self.vehicles_validator.df[['From City*', 'To City*']].copy()
        v_f = self.vehicles_validator.df['From City*'].copy()
        v_t = self.vehicles_validator.df['To City*'].copy()

        v_bool = v_f.isnull() & v_t.isnull()

        if not (v_bool.any()):
            # no vehicles with all from city to city service (Null values)
            valid_bool = v_f.notnull() & v_t.notnull()
            v_tuple = v[valid_bool].apply(tuple, axis=1).to_list()
            if not (set(t)).issubset(set(v_tuple)):
                delta = list(set(t)-set(v_tuple))
                indexes = [t.index(d) for d in delta]
                indexes.sort()
                rows = task.loc[indexes].to_dict(orient='records')
                message = f" No valid Vehicle found for following task (From City, To City)"
                self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.task_validator.SHEET))

    def check_shipment_size(self):
        """
        Validate if shipment weight and volume for a lane is less than maximum capacity available for that lane
        """

        task = self.task_validator.df.copy()
        vehicle = self.vehicles_validator.df.copy()
        #wt_v = {k: max(g['Weight Capacity (kg)'].tolist()) for k, g in vehicle.groupby(['From City*', 'To City*'])}
        #vol_v = {k: max(g['Volume Capacity (cbm)'].tolist()) for k, g in vehicle.groupby(['From City*', 'To City*'])}
        wt_v = vehicle.groupby(['From City*','To City*'], sort=False)['Weight Capacity (kg)'].max()
        vol_v = vehicle.groupby(['From City*','To City*'], sort=False)['Volume Capacity (cbm)'].max()

        col1 = task[["Load (kg)", 'From City*', 'To City*']].copy().dropna()
        col2 = task[["Volume (cbm)", 'From City*', 'To City*']].copy().dropna()

        new1 = pd.merge(col1, wt_v, how='left', left_on=['From City*', 'To City*'], right_on=['From City*','To City*']).dropna()
        new2 = pd.merge(col2, vol_v, how='left', left_on=['From City*', 'To City*'], right_on=['From City*','To City*']).dropna()

        if not new1['Load (kg)'].isnull().values.any():
            invalid_bool1 = new1['Load (kg)'] > new1['Weight Capacity (kg)']
            if invalid_bool1.any():
                indexes = col1[invalid_bool1].index.to_list()
                rows = task.loc[indexes].to_dict(orient='records')
                message = f" weight of the shipments should be less than vehicle weight capacity"
                self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.task_validator.SHEET))

        if not new2['Volume (cbm)'].isnull().values.any():
            invalid_bool2 = new2['Volume (cbm)'] > new2['Volume Capacity (cbm)']
            if invalid_bool2.any():
                indexes = col2[invalid_bool2].index.to_list()
                rows = task.loc[indexes].to_dict(orient='records')
                message = f" volume of the shipments should be less than vehicle volume capacity"
                self.problems.extend(BaseValidator.add_problem(indexes, rows, message, self.task_validator.SHEET))

    @staticmethod
    def filter_vehicle(_vehicles_df, from_city, to_city):
        filter_vehicles_df = pd.DataFrame()
        df = _vehicles_df.copy()
        df_null_bool = df['from_city'].isnull() & df['to_city'].isnull()
        all_vehicles_df = df[df_null_bool]

        not_null_df = df[['from_city', 'to_city']].dropna()
        if not not_null_df.empty:
            fc = not_null_df['from_city'].str.match(from_city)
            tc = not_null_df['to_city'].str.match(to_city)
            if (fc & tc).any():
                filter_vehicles_df = df.loc[not_null_df[fc & tc].index]

        df = pd.concat([all_vehicles_df, filter_vehicles_df], ignore_index=True)

        # Todo Validate vehicle list beforehand
        if df.empty:
            raise ValidationError(message=f"Vehicle not found for [From City: {from_city}, To City: {to_city}]")
        return df

    def validate_sheets(self):
        if not self.validate_file_length():
            self.raise_problems()

        if not self.validate_headers():
            self.raise_problems()

        if not self.validate_mandatory_fields():
            self.raise_problems()

        self.task_validator.process()
        self.vehicles_validator.process()

        self.problems.extend(self.task_validator.problems)
        self.problems.extend(self.vehicles_validator.problems)

        # From City and To City validation
        self.validate_from_to_city_vehicles()

        # Load and Volume with Capacities Validation
        self.demand_with_capacity_validation()

        #  Check shipment size
        self.check_shipment_size()

        # Raise problems
        self.raise_problems()

        self.task_validator.sanitize()
        # Raise problems
        self.problems.extend(self.task_validator.problems)
        self.raise_problems()

        self.vehicles_validator.sanitize()

        # Filter Vehicles
        _vehicles = self.filter_vehicles(
            orders=self.task_validator.new_df.copy(),
            vehicles=self.vehicles_validator.new_df.copy())

        self.vehicles_validator.new_df = _vehicles

        logger.info(
            f"Sheets Validated for rid: {self._rid} # of tasks: {self.task_validator.new_df.shape[0] - 1} # of vehicles:{self.vehicles_validator.new_df.shape[0] - 1}")


    @staticmethod
    def filter_vehicles(orders, vehicles):
        from_city_list = orders['from_city'].values.tolist()
        to_city_list = orders['to_city'].values.tolist()

        cities_list = list(zip(from_city_list, to_city_list))
        vehicles = vehicles.merge(pd.DataFrame(list(set(cities_list)), columns=['from_city', 'to_city']))
        if vehicles.empty:
            raise ValidationError(message=f"Vehicle not found for [From City, To City]")
        return vehicles

    @staticmethod
    def filter_vehicle_cities(from_city, to_city, vehicles):
        from_city_list = []
        to_city_list = []

        from_city_list.append(from_city)
        to_city_list.append(to_city)

        cities_list = list(zip(from_city_list, to_city_list))
        vehicles = vehicles.merge(pd.DataFrame(cities_list, columns=['from_city', 'to_city']))

        return vehicles

    # @profile
    def process_main(self):
        pd.set_option('display.max_columns', None)
        RequestLog(self._rid).write_log(RequestLogMessages.REQUEST_INIT)
        errors = list()
        temp_file_path_list = list()
        summary_df = pd.DataFrame()
        aggregated_df = pd.DataFrame()
        output_df = pd.DataFrame()
        vehicles_df = pd.DataFrame()
        confmatrix_df = pd.DataFrame()
        agg_list = list()
        routes_list = list()
        cm_list = list()
        aggregated_list = list()
        output_list = list()
        confusion_matrix_list = list()
        summary_info = {}

        _task_df = self.task_validator.new_df.copy()
        _vehicles_df = self.vehicles_validator.new_df.copy()

        #Pincodes to coordinates
        self.pincode_to_coordinates(_task_df)

        # groupby df on 'From City*' and 'To City*'
        # task_df_list = self.task_grouping_city(_task_df)

        # Execute Solver
        try:
            rid, temp_path = self.df_to_csv(_task_df, self.task_validator)
            orders = pd.read_csv(temp_path)
            temp_file_path_list.append(temp_path)

            rid, temp_path = self.df_to_csv(_vehicles_df, self.vehicles_validator)
            vehicles = pd.read_csv(temp_path)
            temp_file_path_list.append(temp_path)

            # vehicles = self.filter_vehicles(orders, vehicles)
            # check if request is valid to execute
            try:
                RequestsServices.is_valid_request_to_continue(self._rid)
            except AppException as e:
                raise RequestTerminationException(str(e))
            msg = RequestLogMessages.SOLVER_INIT.format(
                    total_task=orders.shape[0],
                    total_vehicles=vehicles.shape[0],
                )
            RequestLog(self._rid).write_log(msg)
            configuration = SequentialMidMileSolverConfiguration.get_default_sequential_mid_mile_configuration()
            logger.info(f"solver config: {str(configuration)}")
            planner = SolverV5(orders=orders, vehicles=vehicles, config=configuration)
            sequential_data, summary_df, output_df, aggregated_df, vehicles_df, confmatrix_df, status = planner.execute()

            for route_idx in range(len(sequential_data)):
                agg_df = aggregated_df.loc[route_idx]
                orders_df = pd.DataFrame(sequential_data[route_idx]['task'])
                vehicle_df = pd.DataFrame(sequential_data[route_idx]['vehicles'])
                from_city = vehicle_df['from_city'].to_list()[0]
                to_city = vehicle_df['to_city'].to_list()[0]
                all_vehicles = self.filter_vehicle_cities(from_city, to_city, vehicles)
                logger.info("VRP solver starting")
                vrp_configuration = TSPSolverConfiguration.get_default_tsp_configuration()
                planner = SolverVRP(
                    orders=orders_df, vehicles=vehicle_df, agg=agg_df, config=vrp_configuration,
                    timestamp=self.timestamp, request_id=self._rid, all_vehicles=all_vehicles,
                    from_city=from_city, to_city=to_city)
                output, cm, agg_output, summary_tsp = planner.execute()
                summary_info = self.merge_dicts(summary_info, summary_tsp)
                agg_list.append(agg_output)
                routes_list.append(output)
                cm_list.append(cm)
            aggregated_list = self.get_data(agg_list)
            output_list = self.get_data(routes_list)
            confusion_matrix_list = self.get_data(cm_list)

        except AppException as e:
            logger.error(e, exc_info=True)
            errors.append(str(e))
            msg = RequestLogMessages.SOLVER_FAIL.format(
                error_message=str(e)
            )
            RequestLog(self._rid).write_log(msg)

        finally:
            for file_path in temp_file_path_list:
                os.system(f"rm {file_path}")

        sheet_path = self.get_bin_packing_sheet(aggregated_df, output_df, summary_df, vehicles_df, confmatrix_df)
        tsp_sheet_path = self.get_tsp_sheet(aggregated_list, output_list, confusion_matrix_list, summary_info)
        respo = self.get_tsp_response(routes_list=output_list, vrp_path=tsp_sheet_path, bin_packing_path=sheet_path)
        RequestsServices.update(self._rid, dict(response=json.dumps(respo)))

        msg = RequestLogMessages.REQUEST_FINISH.format(
            rid=self._rid,
            count_errors=len(errors)
        )
        RequestLog(self._rid).write_log(msg)

        logger.info(f"Response: details: {respo}")
        if not respo:
            # return failure response
            resp = dict(
                message="Failed",
                code=str(500),
                details=dict(error_details=errors)
            )
            return resp, UploadJobsStatus.FAIL

        respo['details']['error_details'] = errors

        return respo, UploadJobsStatus.SUCCESS

    def process_main_v2(self):
        pd.set_option('display.max_columns', None)
        errors = list()
        temp_file_path_list = list()
        agg_list = list()
        routes_list = list()
        cm_list = list()
        summary_info = {}

        _task_df = self.task_validator.df

        _vehicles_df = self.vehicles_validator.df

        # Pincode to coordinates
        self.pincode_to_coordinates(_task_df)

        # Execute Solver
        try:
            rid, temp_path = self.df_to_csv(_task_df, self.task_validator)
            orders = pd.read_csv(temp_path)
            temp_file_path_list.append(temp_path)

            rid, temp_path = self.df_to_csv(_vehicles_df, self.vehicles_validator)
            vehicles = pd.read_csv(temp_path)
            temp_file_path_list.append(temp_path)

            # vehicles = self.filter_vehicles(orders, vehicles)

            configuration = SequentialMidMileSolverConfiguration.get_default_sequential_mid_mile_configuration()
            logger.info(f"solver config: {str(configuration)}")
            planner = SolverV5(orders=orders, vehicles=vehicles, config=configuration)
            sequential_data, summary_df, output_df, aggregated_df, vehicles_df, confmatrix_df, status = planner.execute()

            if status == "INFEASIBLE":

                raise SolverException(f'Bin Packing Solver could not produce result.')

            for route_idx in range(len(sequential_data)):
                agg_df = aggregated_df.loc[route_idx]
                orders_df = pd.DataFrame(sequential_data[route_idx]['task'])
                vehicle_df = pd.DataFrame(sequential_data[route_idx]['vehicles'])
                from_city = vehicle_df['from_city'].to_list()[0]
                to_city = vehicle_df['to_city'].to_list()[0]
                all_vehicles = self.filter_vehicle_cities(from_city, to_city, vehicles)
                logger.info("VRP solver starting")
                vrp_configuration = TSPSolverConfiguration.get_default_tsp_configuration()
                planner = SolverVRP(
                    orders=orders_df, vehicles=vehicle_df, agg=agg_df, config=vrp_configuration,
                    timestamp=self.timestamp, request_id=self._rid, all_vehicles=all_vehicles,
                    from_city=from_city, to_city=to_city)
                output, cm, agg_output, summary_tsp = planner.execute()
                if summary_tsp.get('status')[0] == SolverStatus.Success.value:
                    summary_info = self.merge_dicts(summary_info, summary_tsp)
                    agg_list.append(agg_output)
                    routes_list.append(output)
                    cm_list.append(cm)
            aggregated_list = self.get_data(agg_list)
            output_list = self.get_data(routes_list)
            confusion_matrix_list = self.get_data(cm_list)

        except AppException as e:
            raise RequestTerminationException(str(e))

        finally:
            for file_path in temp_file_path_list:
                os.system(f"rm {file_path}")

        sheet_path = self.get_bin_packing_sheet_v2(aggregated_df, output_df, summary_df, vehicles_df, confmatrix_df)
        output_sheet_path = self.get_output_sheet(aggregated_list, output_list, confusion_matrix_list, summary_info)
        indent_sheet_path = self.get_indent_sheet(output_list)
        respo = self.get_response_and_update(routes_list=output_list, aggregated_list=aggregated_list,
                                             vrp_path=output_sheet_path, bin_packing_path=sheet_path,
                                             indent_path=indent_sheet_path)

        logger.info(f"Processing Completed for rid {self._rid} , response{respo}")

        if not respo:
            # return failure response
            resp = dict(
                message="Failed",
                code=str(500),
                details=dict(error_details=errors)
            )
            resp['details']['error_details'] = errors
            return resp, UploadJobsStatus.FAIL

        else:

            respo['details']['error_details'] = errors
            return respo, UploadJobsStatus.SUCCESS

    def get_bin_packing_sheet(self, ag_df, o_df, sum_df, veh_df, cm_df):
        excel_file_path = self.get_output_excel_file_path(rid=self._rid)

        with pd.ExcelWriter(excel_file_path) as writer:
            sum_df.to_excel(writer, index_label="Index", sheet_name="Summary")
            ag_df.to_excel(writer, index=False, sheet_name="Aggregated")
            o_df.to_excel(writer, index=False, sheet_name="Output")
            veh_df.to_excel(writer, index_label="Index", sheet_name="Vehicles")
            cm_df.to_excel(writer, index=False, sheet_name="Confusion Matrix")

        return excel_file_path

    def get_bin_packing_sheet_v2(self, ag_df, o_df, sum_df, veh_df, cm_df):
        excel_file_path = self.get_bin_packing_output_excel_file_path(rid=self._rid)

        with pd.ExcelWriter(excel_file_path) as writer:
            sum_df.to_excel(writer, index_label="Index", sheet_name="Summary")
            ag_df.to_excel(writer, index=False, sheet_name="Aggregated")
            o_df.to_excel(writer, index=False, sheet_name="Output")
            veh_df.to_excel(writer, index_label="Index", sheet_name="Vehicles")
            cm_df.to_excel(writer, index=False, sheet_name="Confusion Matrix")

        return excel_file_path

    def response_bin_packing(self, o_path):
        # Get Download Link
        storage_client = PrimaGoogleStorageConfiguration.get_storage_client()
        output_link = storage_client.get_download_link(self._rid, o_path)

        # return response
        response = dict(
            message="Success",
            code=str(200),
            details=dict(
                output_link=output_link
            ))


        os.system(f"rm {o_path}")

        return response

    def get_tsp_sheet(self, ag=None, output=None, cm=None, summary=None):
        vrp = False
        ag_df = export_aggregated_details(ag)
        output_df = export_routes_details(output)
        indent_df = export_indent_details(output)
        cm_df = export_confusion_matrix_details(cm)
        summary_df = export_summary_info(summary, vrp)
        veh_df = export_vehicles_data(ag)

        excel_file_path = self.get_vrp_output_excel_file_path(rid=self._rid)

        with pd.ExcelWriter(excel_file_path) as writer:
            summary_df.to_excel(writer, index_label="Index", sheet_name="Summary")
            ag_df.to_excel(writer, index=False, sheet_name="Aggregated")
            output_df.to_excel(writer, index=False, sheet_name="Output")
            veh_df.to_excel(writer, index_label="Index", sheet_name="Vehicles")
            cm_df.to_excel(writer, index=False, sheet_name="Confusion Matrix")

        return excel_file_path

    def get_output_sheet(self, ag=None, output=None, cm=None, summary=None):
        vrp = False
        ag_df = export_aggregated_details(ag)
        output_df = export_routes_details(output)
        cm_df = export_confusion_matrix_details(cm)
        # indent_df = export_indent_details(output)
        # summary_df = export_summary_info(summary, vrp)
        # veh_df = export_vehicles_data(ag)

        excel_file_path = self.get_output_excel_file_path(rid=self._rid)

        with pd.ExcelWriter(excel_file_path) as writer:
            # summary_df.to_excel(writer, index_label="Index", sheet_name="Summary")
            ag_df.to_excel(writer, index=False, sheet_name="Aggregated")
            output_df.to_excel(writer, index=False, sheet_name="Output")
            # veh_df.to_excel(writer, index_label="Index", sheet_name="Vehicles")
            cm_df.to_excel(writer, index=False, sheet_name="Comparison Matrix")
            # indent_df.to_excel(writer, index=False, sheet_name="Indent Upload File")

        return excel_file_path

    def get_indent_sheet(self, output=None):
        indent_df = export_indent_details(output)

        file_path = self.get_indent_output_excel_file_path(rid=self._rid)

        indent_df.to_csv(file_path, index=False)

        return file_path

    def get_tsp_response(self, routes_list, vrp_path, bin_packing_path):
        # Get Download Link
        storage_client = PrimaGoogleStorageConfiguration.get_storage_client()
        vrp_output_link = storage_client.get_download_link(self._rid, vrp_path)
        bin_packing_link = storage_client.get_download_link(self._rid, bin_packing_path)

        # return response
        response = dict(
            message="Success",
            code=str(200),
            details=dict(
                vrp_output_link=vrp_output_link,
                bin_packing_output_link=bin_packing_link,
                routes=routes_list
            ))

        os.system(f"rm {vrp_path}")
        os.system(f"rm {bin_packing_path}")

        return response

    def get_response_and_update(self, routes_list, aggregated_list, vrp_path, bin_packing_path, indent_path):
        if not routes_list:
            raise AppException("Solver failed to provide result.")
        else:
            # Get Download Link
            planner_type = app_constants.PlanningType.SEQUENTIAL_MID_MILE_PLANNING.value
            storage_client = PrimaGoogleStorageConfiguration.get_storage_client()
            vrp_output_link = storage_client.get_download_link(self._rid, vrp_path)
            bin_packing_link = storage_client.get_download_link(self._rid, bin_packing_path)
            indent_link = storage_client.get_download_link(self._rid, indent_path)

            planning_request_dict = self.get_success_model_dict(aggregated_list)

            order_data_dict = self.get_orders_dict(routes_list)

            planning_result_dict = self.get_planning_result_dict(aggregated_list, order_data_dict, routes_list, self._rid)

            complete_data_dict = dict(
                request_id=self._rid,
                tenant=self.tenant,
                partition=self.partition,
                node=self.node,
                planning_name=planner_type,
                success_data=planning_request_dict,
                order_data=order_data_dict,
                routes_list=routes_list,
                planning_result=planning_result_dict,
            )

            try:
                service = RoutePlanningService()

                result = service.update_complete_request(complete_data_dict)

            except Exception as e:
                raise e

            if result:
                # return response
                response = dict(
                    message="Success",
                    code=str(200),
                    details=dict(
                        routes=routes_list,
                        vrp_output_link=vrp_output_link,
                        indent_link=indent_link,
                        bin_packing_output_link=bin_packing_link
                    ))
            else:
                response = dict()

                os.system(f"rm {vrp_path}")
                os.system(f"rm {bin_packing_path}")
                os.system(f"rm {indent_path}")

        return response

    def process(self, client_type):
        if client_type == app_constants.ClientType.PRIMA.value:
            vehicle_multiplier(self.vehicles_validator.new_df, self.task_validator.new_df, return_df=False)
            return self.process_main()
        elif client_type == app_constants.ClientType.TMS.value:
            self.vehicles_validator.df = vehicle_multiplier(self.vehicles_validator.df, self.task_validator.df, return_df=True)
            return self.process_main_v2()

    def get_task(self):
        col_list = ['request_id', 'task_id', 'to_location', 'consignee_name', 'load', 'volume', 'priority']
        col_mapping = {'request_id': 'request_id',
                       'task_id': 'order_id',
                       'to_location': 'consignee_location',
                       'consignee_name': 'consignee',
                       'load': 'weight_kg',
                       'volume': 'volume_cbm',
                       'priority': 'priority'
                       }
        task_df = self.task_validator.new_df
        task_df = task_df[col_list]
        task_df['priority'] = task_df['priority'].astype("string")
        task_df.rename(columns=col_mapping, inplace=True)
        task_df['planning_request_id'] = self._rid
        task_dict = [{k: v for k, v in m.items() if pd.notnull(v)} for m in task_df.to_dict(orient='records')]
        return task_dict

    def get_vehicles(self):
        col_list = ['request_id', 'vehicle_type_name', 'capacity', 'volumetric_capacity', 'fixed_charges',
                    'num_vehicles']
        col_mapping = {'request_id': 'request_id',
                       'vehicle_type_name': 'vehicle_code',
                       'capacity': 'weight_capacity',
                       'volumetric_capacity': 'volume_capacity',
                       'fixed_charges': 'fixed_cost',
                       'num_vehicles': 'number_of_vehicles'
                       }
        vehicle_df = self.vehicles_validator.new_df
        vehicle_df = vehicle_df[col_list]
        vehicle_df.rename(columns=col_mapping, inplace=True)
        vehicle_df['number_of_vehicles'] = vehicle_df['number_of_vehicles'].astype("int64")
        vehicle_df['planning_request_id'] = self._rid
        vehicle_dict = [{k: v for k, v in m.items() if pd.notnull(v)} for m in vehicle_df.to_dict(orient='records')]
        return vehicle_dict

    def get_validated_input(self):

        excel_file_path = f"{self._rid}_input.xlsx"

        with pd.ExcelWriter(excel_file_path) as writer:
            self.task_validator.new_df.to_excel(writer, index=False, sheet_name="Task")
            self.vehicles_validator.new_df.to_excel(writer, index=False, sheet_name="Vehicles")

        return excel_file_path

    @staticmethod
    def get_success_model_dict(agg_list):
        y = pd.DataFrame(agg_list)
        if not y.empty:
            model_dict = dict(
                planning_end_time=str(datetime.datetime.now()),
                total_cost=float(y['cost'].sum()),
                total_kms=float(y['distance'].sum()),
                time_taken=float(y['time_taken'].sum()),
                stops=int(y['drops'].sum()),
            )
            return model_dict
        else:
            return dict()

    @staticmethod
    def get_planning_result_dict(agg_list, order_data_dict, routes_list, request_id):
        y = pd.DataFrame(agg_list)
        depot = remove_suffix(routes_list[0]['depot'], ' (Depot)')

        if not y.empty:
            time = (y['time_taken'].sum())

            hours = int(time)
            minutes = int((time * 60) % 60)

            if minutes > 0:
                total_duration = f"{hours} hrs {minutes} mins"
            else:
                total_duration = f"{hours} hrs"

            model_dict = dict(
                planning_request_id=request_id,
                total_vehicles=len(y),
                total_routes=len(y),
                total_orders=len(order_data_dict)-len(y),
                total_duration=total_duration,
                total_cost=float(y['cost'].sum()),
                total_kms=float(y['distance'].sum()),
                depot=depot
            )
            return model_dict
        else:
            return dict()

    @staticmethod
    def get_orders_dict(routes_list):
        order_details_dict = list()
        for route in routes_list:
            vehicle_node_dicts = route.get('node_details')
            order_details_dict.extend(vehicle_node_dicts)

        return order_details_dict

    @staticmethod
    def get_path(sheet, rid):
        config = AppConfig()
        rid = f"{rid}"
        path = f"{config.temp_dir}/{rid}_{sheet}.csv"
        return rid, path

    @staticmethod
    def df_to_csv(df, validator):
        rid, path = SequentialMidMileUploadValidation.get_path(validator.SHEET, validator.rid)
        df.to_csv(path, index=False)
        return rid, path

    @staticmethod
    def merge_dicts(a, b):
        new_dict = copy.deepcopy(a)
        for key, value in b.items():
            new_dict.setdefault(key, []).extend(value)
        return new_dict

    @staticmethod
    def get_data(list_of_dicts):
        a = []
        for lists in list_of_dicts:
            for list_data in lists:
                a.append(list_data)
        return a

    @staticmethod
    def get_output_excel_file_path(rid):
        if not rid:
            rid = str(uuid.uuid4())
        return f"{AppConfig().temp_dir}/{rid}_output.xlsx"

    @staticmethod
    def task_grouping_city(task_df):
        task_groupby = task_df.groupby(['from_city', 'to_city'])
        task_df_list = [task_groupby.get_group(x) for x in task_groupby.groups]

        return task_df_list

    @staticmethod
    def pincode_to_coordinates(task_df):
        from_unique_location = list()
        if task_df['from_latitude'].replace(r'^\s*$', np.nan, regex=True).isna().all() \
            and task_df['from_longitude'].replace(r'^\s*$', np.nan, regex=True).isna().all() \
            and task_df['to_latitude'].replace(r'^\s*$', np.nan, regex=True).isna().all() \
                and task_df['to_longitude'].replace(r'^\s*$', np.nan, regex=True).isna().all():

            from_locations = task_df['from_location'].values.tolist()
            from_unique_location.append(from_locations)
            from_lat, from_long = pincode_to_latlong(from_unique_location[0])
            task_df['from_latitude'] = from_lat[0]
            task_df['from_longitude'] = from_long[0]

            to_locations = task_df['to_location'].values.tolist()
            to_lat, to_long = pincode_to_latlong(to_locations)
            task_df['to_latitude'] = to_lat
            task_df['to_longitude'] = to_long

    @staticmethod
    def get_vrp_output_excel_file_path(rid):
        if not rid:
            rid = str(uuid.uuid4())
        return f"{AppConfig().temp_dir}/{rid}_vrp_output.xlsx"

    @staticmethod
    def get_bin_packing_output_excel_file_path(rid):
        if not rid:
            rid = str(uuid.uuid4())
        return f"{AppConfig().temp_dir}/{rid}_bin_packing_output.xlsx"

    @staticmethod
    def get_indent_output_excel_file_path(rid):
        if not rid:
            rid = str(uuid.uuid4())
        return f"{AppConfig().temp_dir}/{rid}_indent_output.csv"





