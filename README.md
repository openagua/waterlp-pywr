# Overview

The San Francisco Water System Model (SFWSM) brings together several existing tools, new methods, and tools to create a model and model platform for the San Francisco Public Utilities Commission (SFPUC) water supply system. This wiki describes 1) these tools and how they work together (the SFWSM structure), 2) the general modeling logic, 3) how SFWSM interacts with other modules related to the Vulnerability Assessment and 4) how to use the SFWSM. It also links to other relevant documentation.

# SFWSM structure

The SFWSM is comprised of 1) a data storage service (Hydra Platform) for input and output, 2) generalized system modeling logic and code, and 3) a graphical user interface to facilitate interacting with the data and model (OpenAgua). Though the GUI is useful, it is not strictly necessary to interact with the model (though doing so will be difficult without at least some graphical tool). However, the data storage service is strictly necessary as of writing.

## Hydra Platform

Hydra Platform is a data management service developed by the water resources group at the University of Manchester and has several essential functions. First, it provides a database model (schema) that organizes water system information in a manner conducive to modeling. Specifically, the database schema organizes water systems into discrete networks, which are comprised of connected nodes (e.g., reservoirs, treatment plants, etc.) and links (e.g., rivers, pipelines, etc.). Each network "resource" (node, link, or the network as a whole), has one or more "attribute" (e.g., "reservoir capacity"), which may also be considered as variables. The whole network may also have variables attached to it, such as climate conditions or any generic variable that cannot be ascribed to a specific individual facility. The database model also allows for scenarios to organize data that might change under different conditions or assumptions. Scenarios may be used for organizing both input and output. In addition to these basic database components, a wide range of supporting database objects and extensions exist to facilitate modeling, such as units (e.g., "cubic feet per second"), variable type (e.g., "timeseries" or "scalar"), and so on.

Second, Hydra Platform provides a web service to interact with the database with convenience functions. Functions include, for example, the ability to add/modify/delete networks, nodes, links, scenarios, and data. Hydra Platform also has a user management system, with login and permissions, facilitated by the web service. The implication is that multiple users and multiple applications can interact with the database. This allows, for example, a GUI to view and/or edit water system data, and a code-only tool to read data from the database to run a model. The multi-user aspect enables online collaboration, so that, for example, researchers at UMass can view the same data online as staff at SFPUC, provided a GUI exists to do so.

## Model
[description of model as it interacts with Hydra; model logic is below]

## OpenAgua

OpenAgua is a web-based application to facilitate interacting with water system data, as organized by and accessed with Hydra Platform, and models.

[docs.openagua.org](http://docs.openagua.org)

[to be continued...]

# Modeling logic

## Generalized model

* LP
* Zero foresight with option of adding foresight
* Core concepts...
* Refer to paper writeup & other documentation
* Links to code (this repository)

## Specific modeling logic

[refer to doc in development w/ SFPUC]

## Interaction with other modules

# How to use

## Online (via OpenAgua)
(refer to OA documentation)

### Create account, etc.
### View/edit data
### Run a model
### View/save results

## Offline (without OpenAgua)

### Setup

### Use with command line interface (CLI)

### Use with Python IDE
(example from PyCharm)
