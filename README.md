sanfran_urbansim
================

This repository contains a simple (but relatively full-featured) example of UrbanSim for San Francisco.

See [the UrbanSim GitHub](https://github.com/udst/urbansim) for more information on UrbanSim.

This example is [documented in detail](https://udst.github.io/urbansim/examples.html#complete-example-san-francisco-urbansim-modules) as part of the UrbanSim documentation.

##Data Caveats

This repository contains the parcel, buildings, and zoning data which are publicly available on the SFGov data portal - https://data.sfgov.org/ - as well as a synthesized dataset of households created using census data and a syntheiszed jobs dataset for understanding the behavior of employment in the region.  The buildings dataset has had two attributes added - `residential_sales_price` and `non_residential_rent` which are synthesized prices.  All data was archived in late 2012.  Because of all these caveats, this example is not intended as a final model of urban growth, but rather as an example of the UrbanSim methodology.  Even with the obfuscation techniques used on the data (which are primarily so that the data can be released publicly), there should be clear patterns that can be discerned using the UrbanSim statistical models.

##To Run this Example

* [Install UrbanSim](https://udst.github.io/urbansim/gettingstarted.html#installation)
* Run `ipython notebook` and execute the included notebooks

The *estimation* notebook allows the estimation of price models and location choices, the *simulation* notebook runs the full UrbanSim simulation, and the *exploration* notebook allows the interactive exploration of the included data using web mapping tools (primarily enabling the interaction between Leaflet and Pandas).
