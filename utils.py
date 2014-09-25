from urbansim.models import RegressionModel, SegmentedRegressionModel, \
    MNLLocationChoiceModel, SegmentedMNLLocationChoiceModel, \
    GrowthRateTransition
from urbansim.developer import sqftproforma, developer
import numpy as np
import pandas as pd
import urbansim.sim.simulation as sim
from urbansim.utils import misc
import os
import json


def get_run_filename():
    return os.path.join(misc.runs_dir(), "run%d.h5" %
                        sim.get_injectable("run_number"))


def change_store(store_name):
    sim.add_injectable("store",
                       pd.HDFStore(os.path.join(misc.data_dir(),
                                                store_name), mode="r"))


def change_scenario(scenario):
    assert scenario in sim.get_injectable("scenario_inputs"), \
        "Invalid scenario name"
    print "Changing scenario to '%s'" % scenario
    sim.add_injectable("scenario", scenario)


def conditional_upzone(scenario, attr_name, upzone_name):
    scenario_inputs = sim.get_injectable("scenario_inputs")
    zoning_baseline = sim.get_table(
        scenario_inputs["baseline"]["zoning_table_name"])
    attr = zoning_baseline[attr_name]
    if scenario != "baseline":
        zoning_scenario = sim.get_table(
            scenario_inputs[scenario]["zoning_table_name"])
        upzone = zoning_scenario[upzone_name].dropna()
        attr = pd.concat([attr, upzone], axis=1).max(skipna=True, axis=1)
    return attr


def enable_logging():
    from urbansim.utils import logutil
    logutil.set_log_level(logutil.logging.INFO)
    logutil.log_to_stream()


def deal_with_nas(df):
    df_cnt = len(df)
    fail = False

    df = df.replace([np.inf, -np.inf], np.nan)
    for col in df.columns:
        s_cnt = df[col].count()
        if df_cnt != s_cnt:
            fail = True
            print "Found %d nas or inf (out of %d) in column %s" % \
                  (df_cnt-s_cnt, df_cnt, col)

    assert not fail, "NAs were found in dataframe, please fix"
    return df


def fill_nas_from_config(dfname, df):
    df_cnt = len(df)
    fillna_config = sim.get_injectable("fillna_config")
    fillna_config_df = fillna_config[dfname]
    for fname in fillna_config_df:
        filltyp, dtyp = fillna_config_df[fname]
        s_cnt = df[fname].count()
        fill_cnt = df_cnt - s_cnt
        if filltyp == "zero":
            val = 0
        elif filltyp == "mode":
            val = df[fname].dropna().value_counts().idxmax()
        elif filltyp == "median":
            val = df[fname].dropna().quantile()
        else:
            assert 0, "Fill type not found!"
        print "Filling column {} with value {} ({} values)".\
            format(fname, val, fill_cnt)
        df[fname] = df[fname].fillna(val).astype(dtyp)
    return df


def to_frame(tbl, join_tbls, cfg, additional_columns=[]):
    join_tbls = join_tbls if isinstance(join_tbls, list) else [join_tbls]
    tables = [tbl] + join_tbls
    cfg = yaml_to_class(cfg).from_yaml(str_or_buffer=cfg)
    tables = [t for t in tables if t is not None]
    columns = misc.column_list(tables, cfg.columns_used()) + additional_columns
    if len(tables) > 1:
        df = sim.merge_tables(target=tables[0].name,
                              tables=tables, columns=columns)
    else:
        df = tables[0].to_frame(columns)
    df = deal_with_nas(df)
    return df


def yaml_to_class(cfg):
    import yaml
    model_type = yaml.load(open(cfg))["model_type"]
    return {
        "regression": RegressionModel,
        "segmented_regression": SegmentedRegressionModel,
        "locationchoice": MNLLocationChoiceModel,
        "segmented_locationchoice": SegmentedMNLLocationChoiceModel
    }[model_type]


def hedonic_estimate(cfg, tbl, join_tbls):
    cfg = misc.config(cfg)
    df = to_frame(tbl, join_tbls, cfg)
    return yaml_to_class(cfg).fit_from_cfg(df, cfg)


def hedonic_simulate(cfg, tbl, join_tbls, out_fname):
    cfg = misc.config(cfg)
    df = to_frame(tbl, join_tbls, cfg)
    price_or_rent, _ = yaml_to_class(cfg).predict_from_cfg(df, cfg)
    tbl.update_col_from_series(out_fname, price_or_rent)


def lcm_estimate(cfg, choosers, chosen_fname, buildings, join_tbls):
    cfg = misc.config(cfg)
    choosers = to_frame(choosers, [], cfg, additional_columns=[chosen_fname])
    alternatives = to_frame(buildings, join_tbls, cfg)
    return yaml_to_class(cfg).fit_from_cfg(choosers,
                                           chosen_fname,
                                           alternatives,
                                           cfg)


def lcm_simulate(cfg, choosers, buildings, join_tbls, out_fname,
                 supply_fname, vacant_fname):
    """
    Simulate the location choices for the specified choosers

    Parameters
    ----------
    cfg : string
        The name of the yaml config file from which to read the location
        choice model.
    choosers : DataFrame
        A dataframe of agents doing the choosing.
    buildings : DataFrame
        A dataframe of buildings which the choosers are locating in and which
        have a supply.
    nodes : DataFrame
        A land use dataset to give neighborhood info around the buildings -
        will be joined to the buildings.
    out_dfname : string
        The name of the dataframe to write the simulated location to.
    out_fname : string
        The column name to write the simulated location to.
    supply_fname : string
        The string in the buildings table that indicates the amount of
        available units there are for choosers, vacant or not.
    vacant_fname : string
        The string in the buildings table that indicates the amount of vacant
        units there will be for choosers.
    """
    cfg = misc.config(cfg)

    choosers_df = to_frame(choosers, [], cfg, additional_columns=[out_fname])
    locations_df = to_frame(buildings, join_tbls, cfg,
                            [supply_fname, vacant_fname])

    available_units = buildings[supply_fname]
    vacant_units = buildings[vacant_fname]

    print "There are %d total available units" % available_units.sum()
    print "    and %d total choosers" % len(choosers)
    print "    but there are %d overfull buildings" % \
          len(vacant_units[vacant_units < 0])

    vacant_units = vacant_units[vacant_units > 0]
    units = locations_df.loc[np.repeat(vacant_units.index,
                             vacant_units.values.astype('int'))].reset_index()

    print "    for a total of %d temporarily empty units" % vacant_units.sum()
    print "    in %d buildings total in the region" % len(vacant_units)

    movers = choosers_df[choosers_df[out_fname] == -1]

    if len(movers) > vacant_units.sum():
        print "WARNING: Not enough locations for movers"
        print "    reducing locations to size of movers for performance gain"
        movers = movers.head(vacant_units.sum())

    new_units, _ = yaml_to_class(cfg).predict_from_cfg(movers, units, cfg)

    # new_units returns nans when there aren't enough units,
    # get rid of them and they'll stay as -1s
    new_units = new_units.dropna()

    # go from units back to buildings
    new_buildings = pd.Series(units.loc[new_units.values][out_fname].values,
                              index=new_units.index)

    choosers.update_col_from_series(out_fname, new_buildings)
    _print_number_unplaced(choosers, out_fname)

    vacant_units = buildings[vacant_fname]
    print "    and there are now %d empty units" % vacant_units.sum()
    print "    and %d overfull buildings" % len(vacant_units[vacant_units < 0])


def simple_relocation(choosers, relocation_rate, fieldname):
    print "Total agents: %d" % len(choosers)
    _print_number_unplaced(choosers, fieldname)

    print "Assinging for relocation..."
    chooser_ids = np.random.choice(choosers.index, size=int(relocation_rate *
                                   len(choosers)), replace=False)
    choosers.update_col_from_series(fieldname,
                                    pd.Series(-1, index=chooser_ids))

    _print_number_unplaced(choosers, fieldname)


def simple_transition(tbl, rate, location_fname):
    transition = GrowthRateTransition(rate)
    df = tbl.to_frame(tbl.local_columns)

    print "%d agents before transition" % len(df.index)
    df, added, copied, removed = transition.transition(df, None)
    print "%d agents after transition" % len(df.index)

    df[location_fname].loc[added] = -1
    sim.add_table(tbl.name, df)


def _print_number_unplaced(df, fieldname):
    print "Total currently unplaced: %d" % \
          df[fieldname].value_counts().get(-1, 0)


def run_feasibility(parcels, parcel_price_callback,
                    parcel_use_allowed_callback, residential_to_yearly=True,
                    historic_preservation=None,
                    config=None, pass_through=[]):
    """
    Execute development feasibility on all parcels

    Parameters
    ----------
    parcels : DataFrame Wrapper
        The data frame wrapper for the parcel data
    parcel_price_callback : function
        A callback which takes each use of the pro forma and returns a series
        with index as parcel_id and value as yearly_rent
    parcel_use_allowed_callback : function
        A callback which takes each form of the pro forma and returns a series
        with index as parcel_id and value and boolean whether the form
        is allowed on the parcel
    residential_to_yearly : boolean (default true)
        Whether to use the cap rate to convert the residential price from total
        sales price per sqft to rent per sqft
    historic_preservation : string
        A filter to apply to the parcels data frame to remove parcels from
        consideration - is typically used to remove parcels with buildings
        older than a certain date
    config : SqFtProFormaConfig configuration object.  Optional.  Defaults to None
    pass_through : list of strings
        Will be passed to the feasibility lookup function - is used to pass
        variables from the parcel dataframe to the output dataframe, usually
        for debugging

    Returns
    -------
    Adds a table called feasibility to the sim object (returns nothing)
    """

    pf = sqftproforma.SqFtProForma(config) if config else sqftproforma.SqFtProForma()

    df = parcels.to_frame()

    if historic_preservation:
        df = df.query(historic_preservation)

    # add prices for each use
    for use in pf.config.uses:
        # assume we can get the 80th percentile price for new development
        df[use] = parcel_price_callback(use, .8)

    # convert from cost to yearly rent
    if residential_to_yearly:
        df["residential"] *= pf.config.cap_rate

    print "Describe of the yearly rent by use"
    print df[pf.config.uses].describe()

    d = {}
    for form in pf.config.forms:
        print "Computing feasibility for form %s" % form
        d[form] = pf.lookup(form, df[parcel_use_allowed_callback(form)],
                            pass_through=pass_through)
        if residential_to_yearly and "residential" in pass_through:
            d[form]["residential"] /= pf.config.cap_rate

    far_predictions = pd.concat(d.values(), keys=d.keys(), axis=1)

    sim.add_table("feasibility", far_predictions)


def run_developer(forms, agents, buildings, supply_fname, parcel_size,
                  ave_unit_size, total_units, feasibility, year=None,
                  target_vacancy=.1, form_to_btype_callback=None,
                  add_more_columns_callback=None, max_parcel_size=2000000,
                  residential=True, bldg_sqft_per_job=400.0,
                  min_unit_size=400, remove_developed_buildings=True,
                  unplace_agents=['households', 'jobs']):
    """
    Run the developer model to pick and build buildings

    Parameters
    ----------
    forms : string or list of strings
        Passed directly dev.pick
    agents : DataFrame Wrapper
        Used to compute the current demand for units/floorspace in the area
    buildings : DataFrame Wrapper
        Used to compute the current supply of units/floorspace in the area
    supply_fname : string
        Identifies the column in buildings which indicates the supply of
        units/floorspace
    parcel_size : Series
        Passed directly to dev.pick
    ave_unit_size : Series
        Passed directly to dev.pick - average residential unit size
    total_units : Series
        Passed directly to dev.pick - total current residential_units /
        job_spaces
    feasibility : DataFrame Wrapper
        The output from feasibility above (the table called 'feasibility')
    year : int
        The year of the simulation - will be assigned to 'year_built' on the
        new buildings
    target_vacancy : float
        The target vacancy rate - used to determine how much to build
    form_to_btype_callback : function
        Will be used to convert the 'forms' in the pro forma to
        'building_type_id' in the larger model
    add_more_columns_callback : function
        Takes a dataframe and returns a dataframe - is used to make custom
        modifications to the new buildings that get added
    max_parcel_size : float
        Passed directly to dev.pick - max parcel size to consider
    min_unit_size : float
        Passed directly to dev.pick - min unit size that is valid
    residential : boolean
        Passed directly to dev.pick - switches between adding/computing
        residential_units and job_spaces
    bldg_sqft_per_job : float
        Passed directly to dev.pick - specified the multiplier between
        floor spaces and job spaces for this form (does not vary by parcel
        as ave_unit_size does)
    remove_redeveloped_buildings : optional, boolean (default True)
        Remove all buildings on the parcels which are being developed on
    unplace_agents : optional : list of strings (default ['households', 'jobs'])
        For all tables in the list, will look for field building_id and set
        it to -1 for buildings which are removed - only executed if
        remove_developed_buildings is true

    Returns
    -------
    Writes the result back to the buildings table and returns the new
    buildings with available debugging information on each new building
    """

    dev = developer.Developer(feasibility.to_frame())

    target_units = dev.\
        compute_units_to_build(len(agents),
                               buildings[supply_fname].sum(),
                               target_vacancy)

    print "{:,} feasible buildings before running developer".format(
          len(dev.feasibility))

    new_buildings = dev.pick(forms,
                             target_units,
                             parcel_size,
                             ave_unit_size,
                             total_units,
                             max_parcel_size=max_parcel_size,
                             min_unit_size=min_unit_size,
                             drop_after_build=True,
                             residential=residential,
                             bldg_sqft_per_job=bldg_sqft_per_job)

    sim.add_table("feasibility", dev.feasibility)

    if new_buildings is None:
        return

    if len(new_buildings) == 0:
        return new_buildings

    if year is not None:
        new_buildings["year_built"] = year

    if not isinstance(forms, list):
        # form gets set only if forms is a list
        new_buildings["form"] = forms

    if form_to_btype_callback is not None:
        new_buildings["building_type_id"] = new_buildings.\
            apply(form_to_btype_callback, axis=1)

    new_buildings["stories"] = new_buildings.stories.apply(np.ceil)

    ret_buildings = new_buildings
    if add_more_columns_callback is not None:
        new_buildings = add_more_columns_callback(new_buildings)

    print "Adding {:,} buildings with {:,} {}".\
        format(len(new_buildings),
               int(new_buildings[supply_fname].sum()),
               supply_fname)

    print "{:,} feasible buildings after running developer".format(
          len(dev.feasibility))

    old_buildings = buildings.to_frame(buildings.local_columns)
    new_buildings = new_buildings[buildings.local_columns]

    if remove_developed_buildings:
        redev_buildings = old_buildings.parcel_id.isin(new_buildings.parcel_id)
        l = len(old_buildings)
        drop_buildings = old_buildings[redev_buildings]
        old_buildings = old_buildings[np.logical_not(redev_buildings)]
        l2 = len(old_buildings)
        if l2-l > 0:
            print "Dropped {} buildings because they were redeveloped".\
                format(l2-l)

        for tbl in unplace_agents:
            agents = sim.get_table(tbl)
            agents = agents.to_frame(agents.local_columns)
            displaced_agents = agents.building_id.isin(drop_buildings.index)
            print "Unplaced {} before: {}".format(tbl, len(agents.query(
                                                  "building_id == -1")))
            agents.building_id[displaced_agents] = -1
            print "Unplaced {} after: {}".format(tbl, len(agents.query(
                                                 "building_id == -1")))
            sim.add_table(tbl, agents)

    all_buildings = dev.merge(old_buildings, new_buildings)

    sim.add_table("buildings", all_buildings)

    return ret_buildings


def write_simulation_output(outname="./run{}_simulation_output.json"):
    """
    Write the full simulation to a file.

    Parameters
    ----------
    outname : string
        A string name of the file, use "{}" notation and the run number will
        be substituted

    Returns
    -------
    Nothing
    """
    d = sim.get_injectable("simulation_output")
    outname = outname.format(sim.get_injectable("run_number"))
    outf = open(outname, "w")
    json.dump(d, outf)
    outf.close()


def add_simulation_output(zones_df, name, year, round=2):
    """
    Pass in a dataframe and this function will store the results in the
    simulation state to write out at the end (to describe how the simulation
    changes over time)

    Parameters
    ----------
    zones_df : DataFrame
        dataframe of indicators whose index is the zone_id and columns are
        indicators describing the simulation
    name : string
        The name of the dataframe to use to differentiate all the sources of
        the indicators
    year : int
        The year to associate with these indicators
    round : int
        The number of decimal places to round to in the output json

    Returnsd
    -------
    Nothing
    """

    key = "simulation_output"

    if key not in sim.list_injectables() or sim.get_injectable(key) is None:
        d = {
            "index": list(zones_df.index),
            "years": []
        }
    else:
        d = sim.get_injectable(key)

    assert d["index"] == list(zones_df.index), "Passing in zones dataframe " \
        "that is not aligned on the same index as a previous dataframe"

    if year not in d["years"]:
        d["years"].append(year)

    for col in zones_df.columns:
        d.setdefault(col, {})
        d[col]["original_df"] = name
        s = zones_df[col]
        dtype = s.dtype
        if dtype == "float64" or dtype == "float32":
            s = s.fillna(0)
            d[col][year] = [float(x) for x in list(s.round(round))]
        elif dtype == "int64" or dtype == "int32":
            s = s.fillna(0)
            d[col][year] = [int(x) for x in list(s)]
        else:
            d[col][year] = list(s)

    sim.add_injectable("simulation_output", d)


def add_parcel_output(new_buildings):
    """
    Add new developments as parcel output

    Parameters
    ----------
    new_buildings : dataframe
        best just to add the new buildings dataframe return from developer model

    Returns
    -------
    Nothing
    """
    if new_buildings is None:
        return

    key = "developments"
    if key in sim.list_injectables():
        # merge with old new buildings
        new_buildings = pd.concat([sim.get_injectable(key), new_buildings]).\
            reset_index(drop=True)

    sim.add_injectable(key, new_buildings)


def write_parcel_output(fname,
                        add_xy=("parcels", "parcel_id", "x", "y", 3740, 4326)):
    """
    Write the parcel-level output to a json file using geopandas - note
    requires geopandas!

    Parameters
    ----------
    fname : string
        The filename to write the output to
    add_xy : tuple
        Used to add x, y values to the output - tuple should contain the name
        of the table which has the x and y values, the field in the development
        table that joins to that table, and the names of the x and y columns -
        the final 2 attributes use pyproj to change the coordinate system -
        set to None, None if you don't want to convert (requires pyproj

    Returns
    -------
    Nothing
    """
    if "developments" not in sim.list_injectables():
        return
    table = sim.get_injectable("developments")

    if add_xy is not None:

        xy_tblname, xy_joinname, x_name, y_name, from_epsg, to_epsg = add_xy
        xy_df = sim.get_table(xy_tblname)
        table[x_name] = misc.reindex(xy_df[x_name], table[xy_joinname])
        table[y_name] = misc.reindex(xy_df[y_name], table[xy_joinname])

        if from_epsg is not None and to_epsg is not None:
            import pyproj
            p1 = pyproj.Proj('+init=epsg:%d' % from_epsg)
            p2 = pyproj.Proj('+init=epsg:%d' % to_epsg)
            x2, y2 = pyproj.transform(p1, p2, table[x_name].values,
                                      table[y_name].values)
            table[x_name], table[y_name] = x2, y2

    table.to_csv(fname.format(sim.get_injectable("run_number")),
                 index_label="development_id")