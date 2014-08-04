sanfran_urbansim
================

This repository contains a simple (but relatively full-featured) example of UrbanSim for San Francisco - see https://github.com/synthicity/urbansim for more information on UrbanSim.

##Data Caveats

This repository contains the parcel, buildings, and zoning data which are publicly available on the data portal - https://data.sfgov.org/ - as well a synthesized dataset of households created using public census dataset and a syntheiszed jobs dataset for understanding the behavior of employment in the region.  The buildings dataset has had two attributes added - `residential_sales_price` and `non_residential_rent` which are syntheisized prices.  All data was archived in 2012.  Because of all these caveats, this example is not intended as a final model of urban growth, but rather as an example of the UrbanSim methodology.  Even with the obfuscation techniques used on the data (which are primarily so that the data can be released publicly), there should be clear patterns that can be discerned using the UrbanSim statistical models.

##To use UrbanSim

* Install Anaconda
* `easy_install urbansim`
* Run `ipython notebook` and explore the included notebooks

The *estimation* notebook allows the estimation of prices and location choices, the *simulation* notebook runs the full UrbanSim simulation, and the *exploration* notebook allows the interactive exploration of the included data using web mapping tools (primarily the interaction between Leaflet and Pandas).
