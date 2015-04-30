import warnings

import orca
import pandas as pd

import assumptions
import utils

warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)


@orca.table('jobs', cache=True)
def jobs(store):
    df = store['jobs']
    df = utils.fill_nas_from_config('jobs', df)
    return df


@orca.table('buildings', cache=True)
def buildings(store):
    df = store['buildings']
    df = df[df.building_type_id > 0]
    df = df[df.building_type_id <= 14]
    df = utils.fill_nas_from_config('buildings', df)
    return df


@orca.table('households', cache=True)
def households(store):
    df = store['households']
    return df


@orca.table('parcels', cache=True)
def parcels(store):
    df = store['parcels']
    return df


# these are shapes - "zones" in the bay area
@orca.table('zones', cache=True)
def zones(store):
    df = store['zones']
    return df


# starts with the same underlying shapefile, but is used later in the simulation
@orca.table('zones_prices', cache=True)
def zones_prices(store):
    df = store['zones']
    return df


# this is the mapping of parcels to zoning attributes
@orca.table('zoning_for_parcels', cache=True)
def zoning_for_parcels(store):
    df = store['zoning_for_parcels']
    df = df.reset_index().drop_duplicates(subset='parcel').set_index('parcel')
    return df


# this is the actual zoning
@orca.table('zoning', cache=True)
def zoning(store):
    df = store['zoning']
    return df


# zoning for use in the "baseline" scenario
# comes in the hdf5
@orca.table('zoning_baseline', cache=True)
def zoning_baseline(zoning, zoning_for_parcels):
    df = pd.merge(zoning_for_parcels.to_frame(),
                  zoning.to_frame(),
                  left_on='zoning',
                  right_index=True)
    return df


orca.broadcast('zones', 'homesales', cast_index=True, onto_on='zone_id')
orca.broadcast('zones', 'costar', cast_index=True, onto_on='zone_id')
orca.broadcast('zones', 'apartments', cast_index=True, onto_on='zone_id')
orca.broadcast('zones', 'buildings', cast_index=True, onto_on='zone_id')
orca.broadcast('zones_prices', 'buildings', cast_index=True, onto_on='zone_id')
orca.broadcast('parcels', 'buildings', cast_index=True, onto_on='parcel_id')
orca.broadcast('buildings', 'households', cast_index=True, onto_on='building_id')
orca.broadcast('buildings', 'jobs', cast_index=True, onto_on='building_id')
