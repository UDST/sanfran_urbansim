import pandas as pd
import assumptions
import utils
import os
import urbansim.sim.simulation as sim
from urbansim.utils import misc

import warnings
warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)
pd.options.mode.chained_assignment = None


@sim.table_source('jobs')
def jobs(store):
    df = store['jobs']
    df = utils.fill_nas_from_config('jobs', df)
    return df


@sim.table_source('buildings')
def buildings(store):
    df = store['buildings']
    df = df[df.building_type_id > 0]
    df = df[df.building_type_id <= 14]
    df = utils.fill_nas_from_config('buildings', df)
    return df


@sim.table_source('households')
def households(store):
    df = store['households']
    return df


@sim.table_source('parcels')
def parcels(store):
    df = store['parcels']
    return df


# these are shapes - "zones" in the bay area
@sim.table_source('zones')
def zones(store):
    df = store['zones']
    return df


# starts with the same underlying shapefile, but is used later in the simulation
@sim.table_source('zones_prices')
def zones_prices(store):
    df = store['zones']
    return df


# this is the mapping of parcels to zoning attributes
@sim.table_source('zoning_for_parcels')
def zoning_for_parcels(store):
    df = store['zoning_for_parcels']
    df = df.reset_index().drop_duplicates(cols='parcel').set_index('parcel')
    return df


# this is the actual baseline zoning, now editable in an excel file
# (the zoning from the h5 file doesn't have all the parameters)
# instead of creating a new h5 file I'm going to add zoning as a csv file
# which is easily browsable in excel and is only 170k bytes
@sim.table_source('zoning')
def zoning(store):
    df = store.zoning
    df2 = pd.read_csv(os.path.join(misc.data_dir(), "baseline_zoning.csv"),
                      index_col="id")
    # this function actually overwrites all columns in the h5 zoning that are
    # available in the csv zoning, but preserves the allowable building types
    for col in df2.columns:
        df[col] = df2[col]
    return df


# zoning for use in the "baseline" scenario
# comes in the hdf5
@sim.table_source('zoning_baseline')
def zoning_baseline(zoning, zoning_for_parcels):
    df = pd.merge(zoning_for_parcels.to_frame(),
                  zoning.to_frame(),
                  left_on='zoning',
                  right_index=True)
    return df


sim.broadcast('zones', 'homesales', cast_index=True, onto_on='zone_id')
sim.broadcast('zones', 'costar', cast_index=True, onto_on='zone_id')
sim.broadcast('zones', 'apartments', cast_index=True, onto_on='zone_id')
sim.broadcast('zones', 'buildings', cast_index=True, onto_on='zone_id')
sim.broadcast('zones_prices', 'buildings', cast_index=True, onto_on='zone_id')
sim.broadcast('parcels', 'buildings', cast_index=True, onto_on='parcel_id')
sim.broadcast('buildings', 'households', cast_index=True, onto_on='building_id')
sim.broadcast('buildings', 'jobs', cast_index=True, onto_on='building_id')
