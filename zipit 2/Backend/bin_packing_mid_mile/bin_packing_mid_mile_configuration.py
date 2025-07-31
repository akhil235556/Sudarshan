from solvers.solver_v4.config import SolverConfigurationV4
from route_planner.utils import env


class BPMidMilerConfigDefault:
    # Boolean Constraints
    VEHICLE_LOAD_CAPACITY_CONSTRAINT = True
    VEHICLE_VOLUME_CAPACITY_CONSTRAINT = True
    VEHICLE_MIN_LOAD_CAPACITY_CONSTRAINT = True
    VEHICLE_MIN_VOLUME_CAPACITY_CONSTRAINT = True
    SKU_EXCLUSION_CONSTRAINT = True



    #  Value
    # Valid Range: (0,1)
    VEHICLE_MIN_LOAD_CAPACITY_PER_RATIO = 0.05
    # Valid Range: (0,1)
    VEHICLE_MIN_VOLUME_CAPACITY_PER_RATIO = 0.05
    NUM_OF_THREADS = 16
    # VEHICLE_REPEAT_FACTOR: integer and > 0
    VEHICLE_REPEAT_FACTOR = 5
    # INTEGER_MULTIPLIER factor to typecast to float to int (Range: >=1 <INT>)
    INTEGER_MULTIPLIER = 1

    SEQUENTIAL_BOOL = False


class BPMidMileSolverConfigVariables:

    @property
    def vehicle_load_capacity_constraint(self):
        return env.get('SOLVER_CONFIG.BP_MID_MILE.VEHICLE_LOAD_CAPACITY_CONSTRAINT') or \
               BPMidMilerConfigDefault.VEHICLE_LOAD_CAPACITY_CONSTRAINT

    @property
    def vehicle_volume_capacity_constraint(self):
        return env.get('SOLVER_CONFIG.BP_MID_MILE.VEHICLE_LOAD_CAPACITY_CONSTRAINT') or \
               BPMidMilerConfigDefault.VEHICLE_VOLUME_CAPACITY_CONSTRAINT

    @property
    def vehicle_min_load_capacity_constraint(self):
        return env.get('SOLVER_CONFIG.BP_MID_MILE.VEHICLE_MIN_WEIGHT_CAPACITY_CONSTRAINT') or \
               BPMidMilerConfigDefault.VEHICLE_MIN_LOAD_CAPACITY_CONSTRAINT

    @property
    def vehicle_min_load_capacity_per_ratio(self):
        return env.get('SOLVER_CONFIG.BP_MID_MILE.VEHICLE_MIN_WEIGHT_CAPACITY_PER_RATIO') or \
               BPMidMilerConfigDefault.VEHICLE_MIN_LOAD_CAPACITY_PER_RATIO

    @property
    def vehicle_min_volume_capacity_constraint(self):
        return env.get('SOLVER_CONFIG.BP_MID_MILE.VEHICLE_MIN_VOLUME_CAPACITY_CONSTRAINT') or \
               BPMidMilerConfigDefault.VEHICLE_MIN_VOLUME_CAPACITY_CONSTRAINT

    @property
    def vehicle_min_volume_capacity_per_ratio(self):
        return env.get('SOLVER_CONFIG.BP_MID_MILE.VEHICLE_MIN_VOLUME_CAPACITY_PER_RATIO') or \
               BPMidMilerConfigDefault.VEHICLE_MIN_VOLUME_CAPACITY_PER_RATIO

    @property
    def sku_exclusion_constraint(self):
        return env.get('SOLVER_CONFIG.BP_MID_MILE.SKU_EXCLUSION_CONSTRAINT') or \
               BPMidMilerConfigDefault.SKU_EXCLUSION_CONSTRAINT

    @property
    def num_of_threads(self):
        return env.get('SOLVER_CONFIG.BP_MID_MILE.NUM_OF_THREADS') or \
               BPMidMilerConfigDefault.NUM_OF_THREADS

    @property
    def vehicle_repeat_factor(self):
        return env.get('SOLVER_CONFIG.BP_MID_MILE.VEHICLE_REPEAT_FACTOR') or \
               BPMidMilerConfigDefault.VEHICLE_REPEAT_FACTOR

    @property
    def integer_multiplier(self):
        return env.get('SOLVER_CONFIG.BP_MID_MILE.INTEGER_MULTIPLIER') or \
               BPMidMilerConfigDefault.INTEGER_MULTIPLIER

    @property
    def sequential_bool(self):
        return env.get('SOLVER_CONFIG.BP_MID_MILE.SEQUENTIAL_BOOL') or \
               BPMidMilerConfigDefault.SEQUENTIAL_BOOL


class BPMidMileSolverConfiguration(object):

    @staticmethod
    def get_default_bp_mid_mile_configuration():
        config_vars = BPMidMileSolverConfigVariables()
        config = SolverConfigurationV4()
        config.vehicle_load_capacity_constraint = config_vars.vehicle_load_capacity_constraint
        config.vehicle_volume_capacity_constraint = config_vars.vehicle_volume_capacity_constraint
        config.vehicle_min_load_capacity_constraint = config_vars.vehicle_min_load_capacity_constraint
        config.vehicle_min_load_capacity_per_ratio = config_vars.vehicle_min_load_capacity_per_ratio
        config.vehicle_min_volume_capacity_constraint = config_vars.vehicle_min_volume_capacity_constraint
        config.vehicle_min_volume_capacity_per_ratio = config_vars.vehicle_min_volume_capacity_per_ratio
        config.sku_exclusion_constraint = config_vars.sku_exclusion_constraint
        config.num_of_threads = config_vars.num_of_threads
        config.vehicle_repeat_factor = config_vars.vehicle_repeat_factor
        config.integer_multiplier = config_vars.integer_multiplier
        config.sequential_bool = config_vars.sequential_bool
        return config
