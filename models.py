import urbansim.sim.simulation as sim
from urbansim.utils import misc
import random
import os
import utils
import dataset
import variables
import time
import numpy as np


@sim.model('rsh_estimate')
def rsh_estimate(buildings, zones):
    return utils.hedonic_estimate("rsh.yaml", buildings, zones)


@sim.model('rsh_simulate')
def rsh_simulate(buildings, zones):
    ret = utils.hedonic_simulate("rsh.yaml", buildings, zones,
                                 "residential_sales_price")
    s = buildings.residential_sales_price
    s[s > 1200] = 1200
    s[s < 250] = 250
    buildings.update_col_from_series("residential_sales_price", s)
    return ret


@sim.model('nrh_estimate')
def nrh_estimate(buildings, zones):
    return utils.hedonic_estimate("nrh.yaml", buildings, zones)


@sim.model('nrh_simulate')
def nrh_simulate(buildings, zones):
    return utils.hedonic_simulate("nrh.yaml", buildings, zones,
                                  "non_residential_rent")


@sim.model('hlcm_estimate')
def hlcm_estimate(households, buildings, zones):
    return utils.lcm_estimate("hlcm.yaml", households, "building_id",
                              buildings, zones)


@sim.model('hlcm_simulate')
def hlcm_simulate(households, buildings, zones):
    return utils.lcm_simulate("hlcm.yaml", households, buildings, zones,
                              "building_id", "residential_units",
                              "vacant_residential_units")


@sim.model('elcm_estimate')
def elcm_estimate(jobs, buildings, zones):
    return utils.lcm_estimate("elcm.yaml", jobs, "building_id",
                              buildings, zones)


@sim.model('elcm_simulate')
def elcm_simulate(jobs, buildings, zones):
    return utils.lcm_simulate("elcm.yaml", jobs, buildings, zones,
                              "building_id", "job_spaces", "vacant_job_spaces")


@sim.model('households_relocation')
def households_relocation(households):
    return utils.simple_relocation(households, .05, "building_id")


@sim.model('jobs_relocation')
def jobs_relocation(jobs):
    return utils.simple_relocation(jobs, .05, "building_id")


@sim.model('households_transition')
def households_transition(households):
    return utils.simple_transition(households, .02, "building_id")


@sim.model('jobs_transition')
def jobs_transition(jobs):
    return utils.simple_transition(jobs, .02, "building_id")


@sim.model('feasibility')
def feasibility(parcels):
    utils.run_feasibility(parcels,
                          variables.parcel_average_price,
                          variables.parcel_is_allowed,
                          historic_preservation='oldest_building > 1940 and '
                                                'oldest_building < 2000',
                          residential_to_yearly=True,
                          pass_through=["oldest_building", "total_sqft",
                                        "max_far", "max_dua", "land_cost",
                                        "residential", "min_max_fars",
                                        "max_far_from_dua", "max_height",
                                        "max_far_from_heights",
                                        "building_purchase_price",
                                        "building_purchase_price_sqft"])


def add_extra_columns(df):
    for col in ["residential_sales_price", "non_residential_rent"]:
        df[col] = 0
    return df


@sim.model('residential_developer')
def residential_developer(feasibility, households, buildings, parcels, year):
    new_buildings = utils.run_developer(
        "residential",
        households,
        buildings,
        "residential_units",
        parcels.parcel_size,
        parcels.ave_unit_size,
        parcels.total_units,
        feasibility,
        year=year,
        target_vacancy=.06,
        min_unit_size=800,
        form_to_btype_callback=sim.get_injectable("form_to_btype_f"),
        add_more_columns_callback=add_extra_columns,
        bldg_sqft_per_job=400.0)

    utils.add_parcel_output(new_buildings)


@sim.model('non_residential_developer')
def non_residential_developer(feasibility, jobs, buildings, parcels, year):
    new_buildings = utils.run_developer(
        ["office", "retail", "industrial"],
        jobs,
        buildings,
        "job_spaces",
        parcels.parcel_size,
        parcels.ave_unit_size,
        parcels.total_job_spaces,
        feasibility,
        year=year,
        target_vacancy=.63,
        form_to_btype_callback=sim.get_injectable("form_to_btype_f"),
        add_more_columns_callback=add_extra_columns,
        residential=False,
        bldg_sqft_per_job=400.0)

    utils.add_parcel_output(new_buildings)


@sim.model("clear_cache")
def clear_cache():
    sim.clear_cache()


# this method is used to push messages from urbansim to websites for live
# exploration of simulation results
@sim.model("pusher")
def pusher(year, run_number, uuid):
    try:
        import pusher
    except:
        # if pusher not installed, just return
        return
    import socket

    p = pusher.Pusher(
        app_id='90082',
        key='2fb2b9562f4629e7e87c',
        secret='2f0a7b794ec38d16d149'
    )
    host = "http://localhost:8765/"
    sim_output = host+"runs/run{}_simulation_output.json".format(run_number)
    parcel_output = host+"runs/run{}_parcel_output.csv".format(run_number)
    p['urbansim'].trigger('simulation_year_completed',
                          {'year': year,
                           'region': 'sanfrancisco',
                           'run_number': run_number,
                           'hostname': socket.gethostname(),
                           'uuid': uuid,
                           'time': time.ctime(),
                           'sim_output': sim_output,
                           'field_name': 'residential_units',
                           'table': 'diagnostic_outputs',
                           'scale': 'jenks',
                           'parcel_output': parcel_output})


@sim.model("diagnostic_output")
def diagnostic_output(households, buildings, zones, year):
    households = households.to_frame()
    buildings = buildings.to_frame()
    zones = zones.to_frame()

    zones['residential_units'] = buildings.groupby('zone_id').\
        residential_units.sum()
    zones['non_residential_sqft'] = buildings.groupby('zone_id').\
        non_residential_sqft.sum()

    zones['retail_sqft'] = buildings.query('general_type == "Retail"').\
        groupby('zone_id').non_residential_sqft.sum()
    zones['office_sqft'] = buildings.query('general_type == "Office"').\
        groupby('zone_id').non_residential_sqft.sum()
    zones['industrial_sqft'] = buildings.query('general_type == "Industrial"').\
        groupby('zone_id').non_residential_sqft.sum()

    zones['average_income'] = households.groupby('zone_id').income.quantile()
    zones['household_size'] = households.groupby('zone_id').persons.quantile()

    zones['residential_sales_price'] = buildings.\
        query('general_type == "Residential"').groupby('zone_id').\
        residential_sales_price.quantile()
    zones['retail_rent'] = buildings[buildings.general_type == "Retail"].\
        groupby('zone_id').non_residential_rent.quantile()
    zones['office_rent'] = buildings[buildings.general_type == "Office"].\
        groupby('zone_id').non_residential_rent.quantile()
    zones['industrial_rent'] = \
        buildings[buildings.general_type == "Industrial"].\
        groupby('zone_id').non_residential_rent.quantile()

    utils.add_simulation_output(zones, "diagnostic_outputs", year)
    utils.write_simulation_output(os.path.join(misc.runs_dir(),
                                               "run{}_simulation_output.json"))
    utils.write_parcel_output(os.path.join(misc.runs_dir(),
                                           "run{}_parcel_output.csv"))
