# Overview

The San Francisco Water System Model (SFWSM) brings together several methods and tools to create a comprehensive modeling platform for the San Francisco Public Utilities Commission (SFPUC) water supply system. This describes these tools generally, indicates where to go for further reading, and gives some specific examples of San Francisco-specific modeling logic.

Note that this does not describe facility-specific operational logic of the SFWSM.

# SFWSM structure

The SFWSM is comprised of:
1. a **data management system** for organizing the water system network and storing data
2. **generalized modeling logic** and code
3. **facility-specific modeling logic** and code
4. a **graphical user interface** (GUI) accessed via [OpenAgua](www.openagua.org), to facilitate interacting with the data and model.

Though the GUI is useful, it is not strictly necessary to run the model, or even edit data. However, the data storage services are necessary as of writing, and editing data without the GUI would be difficult, and describing how to do so is beyond the scope of this readme. Because of the importance of the GUI in practice, OpenAgua is described generally first, followed by data management and modeling details.

# Graphical User Interface

[OpenAgua](https://www.openagua.org), the GUI used for the SFWSM, is a web-based application to facilitate interacting with water system data and models. The purpose of OpenAgua is to view/edit water system data online, run models, and visualize model results. Aside from logging into and using OpenAgua at www.openagua.org, the [OpenAgua documentation](http://docs.openagua.org) is the best place to learn more, although it is a work-in-progress.

# Data management

Data is stored in three places in the SFWSM:

1. a database managed by **Hydra Platform**, a data management service developed by the water resources group at the University of Manchester
2. a database managed by **OpenAgua**
3. data files such as CSV files stored on **Amazon Web Services (AWS) S3**.

## Hydra Platform data

Hydra Platform provides a database model (schema) that organizes water system information in a manner conducive to modeling. Specifically, the database schema organizes water systems into discrete networks, which are comprised of connected nodes (e.g., reservoirs, treatment plants, etc.) and links (e.g., rivers, pipelines, etc.). Each network *resource* (node, link, or the network as a whole), has one or more *attributes* (e.g., "reservoir capacity"), which may also be called *variables*. The whole network may also have variables attached to it, such as climate conditions or any generic variable that cannot be ascribed to a specific individual facility.

In addition to organizing the network, Hydra Platform also stores data values associated with resources and their attributes. These may be as scalars, time series, arrays, or descriptors.

**IMPORTANT**: Though Hydra Platform stores primary data (scalars, time series, etc.), OpenAgua introduces the concept of *functions*. These functions are written in Python, and return a value in any given time step. This concept is discussed below, along with examples specific to the SFWSM. Documentation about the functions, which are not unique to the SFWSM, can be found at [https://openagua.github.io/waterlp-general](https://openagua.github.io/waterlp-general) (as with the general OpenAgua documentation, this is a work-in-progress).

The database model also has *scenarios* to organize data that might change under different conditions or assumptions. Scenarios may be used for organizing both input and output. In the OpenAgua interface, scenarios are (currently) organized as *options*, *portfolios*, and *scenarios*, where options are management interventions and scenarios are external conditions (e.g., climate).

Hydra Platform also provides a web API to create and edit data. Though this is not elaborated on here, this API is used directly by the SFWSM when running a model. Specifically, the API is used to read data and write results.

## OpenAgua data

OpenAgua stores supplemental data in addition to core network structure and dataset values. Example supplemental data includes:

* Chart/table information for viewing results and saving favorite results
* Model run information, to connect the generalized model engine with OpenAgua

This data is only relevant when using the OpenAgua application

## AWS S3 data

Sometimes it makes more sense to store water system data in CSV files instead of in the database. The best example of this is hydrologic input data derived from the hydrologic models. It is far more convenient to store inflow time series, for example, as CSV files rather than as database structures. To address this, OpenAgua connects with [AWS S3](https://aws.amazon.com/s3/) to allow users to save CSV files online, via the OpenAgua interface. With the appropriate credentials, this data can then be read by the generalized model engine during a model run.

# Modeling logic

The general modeling approach for the SFWSM (and OpenAgua generally) is to use a generalized model engine with facility-specific data and operating rules.

## Generalized model logic

The water allocation routine consists of a benefit-maximizing linear programming model. The generalized logic, which includes a fairly standard set of facility types, operational decision variables, and constraints, is derived from that described in detail for the default OpenAgua model generally at https://openagua.github.io/waterlp-general/allocation-logic/.

The generalized logic is set up in such a way that only one file changes between the default OpenAgua model logic and that of the SFWSM. Specifically, it is the `pyomo_model.py` file (https://projects.cloudwaterlab.com/UMass/SFPUC/water-system-model/blob/master/model/model.py) in the source model that differs somewhat.

The difference between the OpenAgua default model and that of the SFWSM is not major, however, consisting primarily of new facility types that do not currently have any modeling significance. For example, whereas the default OpenAgua model includes generic *conveyances*, the SFWSM includes *pipelines*, *tunnels* and *aqueducts*.

**TODO**: More specific SFWSM deviations will be documented here.

## Facility-specific modeling logic

Facility-specific modeling logic is defined by populating variables with data specific to the RWS. However, populating this data is not trivial, and represents the core modeling task that makes the SFWSM simulated water operations in a meaningful way. There are several model customization and logic definition tasks worth highlighting.

### Network schematic

The network schematic, which defines system resource types and how they are connected, is edited manually via the OpenAgua GUI.

### Resource types variable definition

System resource types and their variables (*attributes* in Hydra Platform) are modified as needed in the OpenAgua template editor, as [described here](https://docs.openagua.org/how-to-use/user-guide/configuration/network-templates). The template editor is where new variables may be added to a resource type, and where new global variables can be defined. Global variables are particularly valuable, and are quite flexible. For example, a global variable called *climate_realization* can (and will!) be added to specify the climate realization used for a particular model run.

When editing variables, it is important to correctly identify their scope and other parameters (see template documentation), as defining a variable incorrectly can lead to unintended modeling outcomes.

**Important**: The generalized model is built completely independently from the resource types and variables described within OpenAgua (i.e., within Hydra Platform). For this reason, it is critical that the resource types and variables match with what the model expects. The practical implication of this is that you can add a variable, but it might not mean anything in the model. Conversely, deleting a variable in the SFWSM template can result in an error in the model if the generalized model expects that variable. Conversely, the generalized model is set up in such a way that any arbitrary new resource type (e.g., *Groundwater Recharge Area*) will be accounted for in the system structure, and likely not cause any error. (Having said that, you should probably not add/delete resource types unless you know what you are doing.)

### Facility-specific data

Entering facility-specific data is generally done through the OpenAgua interface, although the Hydra Platform API can also be used to add/edit/delete data.

See the main OpenAgua documentation for how to [view and edit data](https://docs.openagua.org/how-to-use/user-guide/setup-model/view-edit-data).

Operational logic for spacific facilities is described in a [separate Google Doc](https://docs.google.com/document/d/19qTpzT-JEKpwmsF28UgYSVeF0gQW1batZOGd7pwBsF8/edit?usp=sharing)

### Example input

The implementation of several representative (and important) water system operations are described in the water system model wiki.

The purpose is to supplement the generalized water system operation [documentation](https://openagua.github.io/waterlp-general) with SFPUC-specific examples.

# Running the model

In most cases you will run the model simply by clicking a "run" button in OpenAgua. This entails setting up a new model run, as [described here](https://docs.openagua.org/how-to-use/user-guide/running-models).

There are multiple ways to set up the model to run it in other ways, such as from a development computer. If you are an administrative user, or otherwise interested in running the model in a more manual mode, please see the [installation instructions](https://github.com/openagua/waterlp-general/blob/master/README.md) for the generalized model engine on GitHub.

Once a model is run, the results may be viewed as described in the main [OpenAgua documentation](https://docs.openagua.org/how-to-use/user-guide/view-export-results).

# Contributing

**TODO**: Add note on development and link to OpenAgua documentation.