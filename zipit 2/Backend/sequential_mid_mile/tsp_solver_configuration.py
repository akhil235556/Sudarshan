from solvers.solver_v4.config import SolverConfigurationTSP
from route_planner.utils import env


class TSPSolverConfigDefault:
    PARTIAL_PLANNING = False
    SERVICEABLE_CONSTRAINT = False
    PRIORITY_CONSTRAINT = False
    VEHICLE_WEIGHT_CAPACITY_CONSTRAINT = True
    VEHICLE_VOLUME_CAPACITY_CONSTRAINT = True
    SLA_TIME_WINDOW_CONSTRAINT = True
    OPERATING_TIME_WINDOW_CONSTRAINT = False
    SEARCH_TIME_LIMIT = None
    NODE_DROP_PENALTY = 1000
    WELDED_WEIGHT_CAPACITY_CONSTRAINT = False
    WELDED_VOLUME_CAPACITY_CONSTRAINT = False
    MAX_LENGTH_CONSTRAINT = False
    MAX_NODE_VISITS_CONSTRAINT = False
    MAX_DROP_DISTANCE_CONSTRAINT = False
    MID_MILE_COST = False
    SEQUENTIAL_BOOL = True

class TSPSolverConfigVariables:

    @property
    def partial_planning(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.PARTIAL_PLANNING') or \
               TSPSolverConfigDefault.SERVICEABLE_CONSTRAINT
    @property
    def serviceable_constraint(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.SERVICEABLE_CONSTRAINT') or \
               TSPSolverConfigDefault.SERVICEABLE_CONSTRAINT

    @property
    def priority_constraint(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.PRIORITY_CONSTRAINT') or \
               TSPSolverConfigDefault.PRIORITY_CONSTRAINT

    @property
    def vehicle_weight_capacity_constraint(self):
        return env.get('SOLVER_CONFIG.CONSOLIDATED_VARIABLE_COST.VEHICLE_WEIGHT_CAPACITY_CONSTRAINT') or \
               TSPSolverConfigDefault.VEHICLE_WEIGHT_CAPACITY_CONSTRAINT

    @property
    def vehicle_volume_capacity_constraint(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.VEHICLE_VOLUME_CAPACITY_CONSTRAINT') or \
               TSPSolverConfigDefault.VEHICLE_VOLUME_CAPACITY_CONSTRAINT

    @property
    def sla_time_window_constraint(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.SLA_TIME_WINDOW_CONSTRAINT') or \
               TSPSolverConfigDefault.SLA_TIME_WINDOW_CONSTRAINT

    @property
    def operating_time_window_constraint(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.OPERATING_TIME_WINDOW_CONSTRAINT') or \
               TSPSolverConfigDefault.OPERATING_TIME_WINDOW_CONSTRAINT

    @property
    def search_time_limit(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.SEARCH_TIME_LIMIT') or \
               TSPSolverConfigDefault.SEARCH_TIME_LIMIT

    @property
    def node_drop_penalty(self):
        return env.get('SOLVER_CONFIG.CONSOLIDATED_VARIABLE_COST.NODE_DROP_PENALTY') or \
               TSPSolverConfigDefault.NODE_DROP_PENALTY

    @property
    def welded_weight_capacity_constraint(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.WELDED_WEIGHT_CAPACITY_CONSTRAINT') or \
               TSPSolverConfigDefault.WELDED_WEIGHT_CAPACITY_CONSTRAINT

    @property
    def welded_volume_capacity_constraint(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.WELDED_VOLUME_CAPACITY_CONSTRAINT') or \
               TSPSolverConfigDefault.WELDED_VOLUME_CAPACITY_CONSTRAINT

    @property
    def max_node_visits_constraint(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.MAX_NODE_VISITS_CONSTRAINT') or \
               TSPSolverConfigDefault.MAX_NODE_VISITS_CONSTRAINT

    @property
    def max_length_constraint(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.MAX_LENGTH_CONSTRAINT') or \
               TSPSolverConfigDefault.MAX_LENGTH_CONSTRAINT

    @property
    def max_drop_distance_constraint(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.MAX_DROP_DISTANCE_CONSTRAINT') or \
               TSPSolverConfigDefault.MAX_DROP_DISTANCE_CONSTRAINT

    @property
    def mid_mile_cost(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.MID_MILE_COST') or \
               TSPSolverConfigDefault.MID_MILE_COST

    @property
    def sequential_bool(self):
        return env.get('SOLVER_CONFIG.TSP_SOLVER.SEQUENTIAL_BOOL') or \
               TSPSolverConfigDefault.SEQUENTIAL_BOOL


class TSPSolverConfiguration(object):

    @staticmethod
    def get_default_tsp_configuration():
        config_vars = TSPSolverConfigVariables()
        config = SolverConfigurationTSP()
        config.sku_planning = False
        config.costing_fixed = True
        config.costing_per_km = False
        config.costing_per_kg = False
        config.daily_run = False

        config.partial_planning = config_vars.partial_planning
        config.serviceable_constraint = config_vars.serviceable_constraint
        config.priority_constraint = config_vars.priority_constraint
        config.vehicle_weight_capacity_constraint = config_vars.vehicle_weight_capacity_constraint
        config.vehicle_volume_capacity_constraint = config_vars.vehicle_volume_capacity_constraint
        config.sla_time_window_constraint = config_vars.sla_time_window_constraint
        config.operating_time_window_constraint = config_vars.operating_time_window_constraint
        config.search_time_limit = config_vars.search_time_limit
        config.node_drop_penalty = config_vars.node_drop_penalty
        config.max_length_constraint = config_vars.max_length_constraint
        config.max_node_visits_constraint = config_vars.max_node_visits_constraint
        config.welded_weight_capacity_constraint = config_vars.welded_weight_capacity_constraint
        config.welded_volume_capacity_constraint = config_vars.welded_volume_capacity_constraint
        config.max_drop_distance_constraint = config_vars.max_drop_distance_constraint
        config.mid_mile_cost = config_vars.mid_mile_cost

        # Data Configurations
        config.products_flag = False
        config.priority_flag = False
        config.task_id = True
        config.order_id = True
        config.sequential_bool = True
        return config
