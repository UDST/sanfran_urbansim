import numpy as np
import orca
import pandas as pd
from urbansim.utils import misc

import dataset
import utils


#####################
# ZONES VARIABLES
#####################


@orca.column('zones', 'sum_residential_units')
def sum_residential_units(buildings):
    return buildings.residential_units.groupby(buildings.zone_id).sum().apply(np.log1p)


@orca.column('zones', 'sum_job_spaces')
def sum_nonresidential_units(buildings):
    return buildings.job_spaces.groupby(buildings.zone_id).sum().apply(np.log1p)


@orca.column('zones', 'population')
def population(households, zones):
    s = households.persons.groupby(households.zone_id).sum().apply(np.log1p)
    return s.reindex(zones.index).fillna(0)


@orca.column('zones', 'jobs')
def jobs(jobs):
    return jobs.zone_id.groupby(jobs.zone_id).size().apply(np.log1p)


@orca.column('zones', 'ave_lot_sqft')
def ave_lot_sqft(buildings, zones):
    s = buildings.unit_lot_size.groupby(buildings.zone_id).quantile().apply(np.log1p)
    return s.reindex(zones.index).fillna(s.quantile())


@orca.column('zones', 'ave_income')
def ave_income(households, zones):
    s = households.income.groupby(households.zone_id).quantile().apply(np.log1p)
    return s.reindex(zones.index).fillna(s.quantile())


@orca.column('zones', 'hhsize')
def hhsize(households, zones):
    s = households.persons.groupby(households.zone_id).quantile().apply(np.log1p)
    return s.reindex(zones.index).fillna(s.quantile())


@orca.column('zones', 'ave_unit_sqft')
def ave_unit_sqft(buildings, zones):
    s = buildings.unit_sqft[buildings.general_type == "Residential"]\
        .groupby(buildings.zone_id).quantile().apply(np.log1p)
    return s.reindex(zones.index).fillna(s.quantile())


@orca.column('zones', 'sfdu')
def sfdu(buildings, zones):
    s = buildings.residential_units[buildings.building_type_id == 1]\
        .groupby(buildings.zone_id).sum().apply(np.log1p)
    return s.reindex(zones.index).fillna(0)


@orca.column('zones', 'poor')
def poor(households, zones):
    s = households.persons[households.income < 40000]\
        .groupby(households.zone_id).sum().apply(np.log1p)
    return s.reindex(zones.index).fillna(0)


@orca.column('zones', 'renters')
def renters(households, zones):
    s = households.persons[households.tenure == 2]\
        .groupby(households.zone_id).sum().apply(np.log1p)
    return s.reindex(zones.index).fillna(0)


@orca.column('zones', 'zone_id')
def zone_id(zones):
    return zones.index


@orca.column('zones_prices', 'residential')
def residential(buildings):
    return buildings\
        .residential_sales_price[buildings.general_type == "Residential"]\
        .groupby(buildings.zone_id).quantile()


@orca.column('zones_prices', 'retail')
def retail(buildings):
    return buildings.non_residential_rent[buildings.general_type == "Retail"]\
        .groupby(buildings.zone_id).quantile()


@orca.column('zones_prices', 'office')
def office(buildings):
    return buildings.non_residential_rent[buildings.general_type == "Office"]\
        .groupby(buildings.zone_id).quantile()


@orca.column('zones_prices', 'industrial')
def industrial(buildings):
    return buildings.non_residential_rent[buildings.general_type == "Industrial"]\
        .groupby(buildings.zone_id).quantile()


@orca.column('zones_prices', 'zone_id')
def zone_id(zones):
    return zones.index


#####################
# BUILDINGS VARIABLES
#####################


@orca.column('buildings', 'zone_id', cache=True, cache_scope='iteration')
def zone_id(buildings, parcels):
    return misc.reindex(parcels.zone_id, buildings.parcel_id)


@orca.column('buildings', 'general_type', cache=True, cache_scope='iteration')
def general_type(buildings, building_type_map):
    return buildings.building_type_id.map(building_type_map)


@orca.column('buildings', 'unit_sqft', cache=True, cache_scope='iteration')
def unit_sqft(buildings):
    return buildings.building_sqft / buildings.residential_units.replace(0, 1)


@orca.column('buildings', 'unit_lot_size', cache=True, cache_scope='iteration')
def unit_lot_size(buildings, parcels):
    return misc.reindex(parcels.parcel_size, buildings.parcel_id) / \
        buildings.residential_units.replace(0, 1)


@orca.column('buildings', 'sqft_per_job', cache=True, cache_scope='iteration')
def sqft_per_job(buildings, building_sqft_per_job):
    return buildings.building_type_id.fillna(-1).map(building_sqft_per_job)


@orca.column('buildings', 'job_spaces', cache=True, cache_scope='iteration')
def job_spaces(buildings):
    return (buildings.non_residential_sqft /
            buildings.sqft_per_job).fillna(0).astype('int')


@orca.column('buildings', 'vacant_residential_units')
def vacant_residential_units(buildings, households):
    return buildings.residential_units.sub(
        households.building_id.value_counts(), fill_value=0)


@orca.column('buildings', 'vacant_job_spaces')
def vacant_residential_units(buildings, jobs):
    return buildings.job_spaces.sub(
        jobs.building_id.value_counts(), fill_value=0)


#####################
# HOUSEHOLDS VARIABLES
#####################


@orca.column(
    'households', 'income_quartile', cache=True, cache_scope='iteration')
def income_quartile(households):
    return pd.Series(pd.qcut(households.income, 4, labels=False),
                     index=households.index)


@orca.column('households', 'zone_id', cache=True, cache_scope='iteration')
def zone_id(households, buildings):
    return misc.reindex(buildings.zone_id, households.building_id)


#####################
# JOBS VARIABLES
#####################


@orca.column('jobs', 'zone_id', cache=True, cache_scope='iteration')
def zone_id(jobs, buildings):
    return misc.reindex(buildings.zone_id, jobs.building_id)


#####################
# PARCELS VARIABLES
#####################


def parcel_average_price(use):
    return misc.reindex(orca.get_table('zones_prices')[use],
                        orca.get_table('parcels').zone_id)


def parcel_is_allowed(form):
    form_to_btype = orca.get_injectable("form_to_btype")
    # we have zoning by building type but want
    # to know if specific forms are allowed
    allowed = [orca.get_table('zoning_baseline')
               ['type%d' % typ] == 't' for typ in form_to_btype[form]]
    return pd.concat(allowed, axis=1).max(axis=1).\
        reindex(orca.get_table('parcels').index).fillna(False)


@orca.column('parcels', 'max_far', cache=True)
def max_far(parcels, scenario):
    return utils.conditional_upzone(scenario, "max_far", "far_up").\
        reindex(parcels.index).fillna(0)


@orca.column('parcels', 'max_height', cache=True, cache_scope='iteration')
def max_height(parcels, zoning_baseline):
    return zoning_baseline.max_height.reindex(parcels.index).fillna(0)


@orca.column('parcels', 'parcel_size', cache=True, cache_scope='iteration')
def parcel_size(parcels):
    return parcels.shape_area * 10.764


@orca.column('parcels', 'total_units', cache=True, cache_scope='iteration')
def total_units(parcels, buildings):
    return buildings.residential_units.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)


@orca.column('parcels', 'total_job_spaces', cache=True, cache_scope='iteration')
def total_job_spaces(parcels, buildings):
    return buildings.job_spaces.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)


@orca.column('parcels', 'total_sqft', cache=True, cache_scope='iteration')
def total_sqft(parcels, buildings):
    return buildings.building_sqft.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)


@orca.column('parcels', 'land_cost')
def land_cost(parcels):
    # TODO
    # this needs to account for cost for the type of building it is
    return (parcels.total_sqft * parcel_average_price("residential")).\
        reindex(parcels.index).fillna(0)


@orca.column('parcels', 'ave_unit_size')
def ave_unit_size(parcels, zones):
    return misc.reindex(zones.ave_unit_sqft, parcels.zone_id)
