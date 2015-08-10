import random

import orca

import dataset
import utils
import variables


@orca.injectable()
def year(iter_var):
    return iter_var


@orca.step('rsh_estimate')
def rsh_estimate(buildings, zones):
    return utils.hedonic_estimate("rsh.yaml", buildings, zones)


@orca.step('rsh_simulate')
def rsh_simulate(buildings, zones):
    return utils.hedonic_simulate("rsh.yaml", buildings, zones,
                                  "residential_sales_price")


@orca.step('nrh_estimate')
def nrh_estimate(buildings, zones):
    return utils.hedonic_estimate("nrh.yaml", buildings, zones)


@orca.step('nrh_simulate')
def nrh_simulate(buildings, zones):
    return utils.hedonic_simulate("nrh.yaml", buildings, zones,
                                  "non_residential_rent")


@orca.step('hlcm_estimate')
def hlcm_estimate(households, buildings, zones):
    return utils.lcm_estimate("hlcm.yaml", households, "building_id",
                              buildings, zones)


@orca.step('hlcm_simulate')
def hlcm_simulate(households, buildings, zones):
    return utils.lcm_simulate("hlcm.yaml", households, buildings, zones,
                              "building_id", "residential_units",
                              "vacant_residential_units")


@orca.step('elcm_estimate')
def elcm_estimate(jobs, buildings, zones):
    return utils.lcm_estimate("elcm.yaml", jobs, "building_id",
                              buildings, zones)


@orca.step('elcm_simulate')
def elcm_simulate(jobs, buildings, zones):
    return utils.lcm_simulate("elcm.yaml", jobs, buildings, zones,
                              "building_id", "job_spaces", "vacant_job_spaces")


@orca.step('households_relocation')
def households_relocation(households):
    return utils.simple_relocation(households, .05, "building_id")


@orca.step('jobs_relocation')
def jobs_relocation(jobs):
    return utils.simple_relocation(jobs, .05, "building_id")


@orca.step('households_transition')
def households_transition(households):
    return utils.simple_transition(households, .05, "building_id")


@orca.step('jobs_transition')
def jobs_transition(jobs):
    return utils.simple_transition(jobs, .05, "building_id")


@orca.step('feasibility')
def feasibility(parcels):
    utils.run_feasibility(parcels,
                          variables.parcel_average_price,
                          variables.parcel_is_allowed,
                          residential_to_yearly=True)


def random_type(form):
    form_to_btype = orca.get_injectable("form_to_btype")
    return random.choice(form_to_btype[form])


def add_extra_columns(df):
    for col in ["residential_sales_price", "non_residential_rent"]:
        df[col] = 0
    return df


@orca.step('residential_developer')
def residential_developer(feasibility, households, buildings, parcels, year):
    utils.run_developer("residential",
                        households,
                        buildings,
                        "residential_units",
                        parcels.parcel_size,
                        parcels.ave_unit_size,
                        parcels.total_units,
                        feasibility,
                        year=year,
                        target_vacancy=.15,
                        form_to_btype_callback=random_type,
                        add_more_columns_callback=add_extra_columns,
                        bldg_sqft_per_job=400.0)


@orca.step('non_residential_developer')
def non_residential_developer(feasibility, jobs, buildings, parcels, year):
    utils.run_developer(["office", "retail", "industrial"],
                        jobs,
                        buildings,
                        "job_spaces",
                        parcels.parcel_size,
                        parcels.ave_unit_size,
                        parcels.total_job_spaces,
                        feasibility,
                        year=year,
                        target_vacancy=.15,
                        form_to_btype_callback=random_type,
                        add_more_columns_callback=add_extra_columns,
                        residential=False,
                        bldg_sqft_per_job=400.0)
