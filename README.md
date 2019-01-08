# Overview

The San Francisco Water System Model (SFWSM) brings together several methods and tools to create a comprehensive modeling platform for the San Francisco Public Utilities Commission (SFPUC) water supply system. This describes these tools generally, indicates where to go for further reading, and gives some specific examples of San Francisco-specific modeling logic.

# SFWSM structure

The SFWSM is comprised of:
1. a data management system for organizing the water system network storing data
2. generalized modeling logic and code
3. facility-specific modeling logic and code
4. a graphical user interface (GUI) accessed via [OpenAgua](www.openagua.org), to facilitate interacting with the data and model.

Though the GUI is useful, it is not strictly necessary to run the model, or even edit data. However, the data storage service is strictly necessary as of writing, and editing data without the GUI would be difficult and is beyond the scope of this readme. Because of the importance of the GUI in practice, OpenAgua is discussed generally first, followed by data management and modeling details.

# OpenAgua

[OpenAgua](https://www.openagua.org) is a web-based application to facilitate interacting with water system data and models. The purpose of OpenAgua is to be able to view/edit water system data online, run models, and visualize model results. Aside from logging into and using OpenAgua at www.openagua.org, the [OpenAgua documentation](http://docs.openagua.org) is the best place to learn more, although it is a work-in-progress.

# Data management

Data is stored in three places in the SFWSM:

1. a database managed by **Hydra Platform**, a data management service developed by the water resources group at the University of Manchester
2. a database managed by **OpenAgua**
3. data files such as CSV files stored on **Amazon Web Services S3**.

## Hydra Platform data

Hydra Platform provides a database model (schema) that organizes water system information in a manner conducive to modeling. Specifically, the database schema organizes water systems into discrete networks, which are comprised of connected nodes (e.g., reservoirs, treatment plants, etc.) and links (e.g., rivers, pipelines, etc.). Each network *resource* (node, link, or the network as a whole), has one or more *attributes* (e.g., "reservoir capacity"), which may also be called *variables*. The whole network may also have variables attached to it, such as climate conditions or any generic variable that cannot be ascribed to a specific individual facility.

In addition to organizing the network, Hydra Platform also stores data values associated with resources and their attributes. These may be as scalars, time series, arrays, or descriptors.

**IMPORTANT**: Though Hydra Platform stores primary data (scalars, time series, etc.), OpenAgua introduces the concept of *functions*. These functions are essentially Python functions, which return a value in any given time step. This concept--and examples--are discussed below. Documentation about the functions, which are not unique to the SFWSM, can be found at [https://openagua.github.io/waterlp-general](https://openagua.github.io/waterlp-general) (as with the general OpenAgua documentation, this is a work-in-progress).

The database model also has *scenarios* to organize data that might change under different conditions or assumptions. Scenarios may be used for organizing both input and output. In the OpenAgua interface, scenarios are (currently) organized as *options*, *portfolios*, and *scenarios*, where options are management interventions and scenarios are external conditions (e.g., climate).

Hydra Platform also provides a web API to create and edit data. Though this is not elaborated on here, this API is used, for example, when running a model.

## OpenAgua data

OpenAgua stores supplemental data in addition to core network structure and dataset values. Example supplemental data includes:

* Chart/table information for viewing results and saving favorite results
* Model run information, to connect the generalized model engine with OpenAgua

# Modeling logic

## Generalized model

* LP
* Zero foresight with option of adding foresight
* Core concepts...
* Refer to paper writeup & other documentation
* Links to code (this repository)

## Specific modeling logic

To the extent possible, specific modeling logic is not included in this generalized code, but rather entered into the Hydra database via the OpenAgua GUI.

That modeling logic is described in a [separate Google Doc](https://docs.google.com/document/d/19qTpzT-JEKpwmsF28UgYSVeF0gQW1batZOGd7pwBsF8/edit?usp=sharing)

### Examples

# Running the model

There are three ways to set up the model: from the OpenAgua interface, with the command line interface, and with a Python integrated development environment.

## from within OpenAgua

This mode is used when the model will be run using the OpenAgua interface, the most likely scenario.

## with the command line interface (CLI)

This mode is likely seldom used, but nonetheless can be necessary, such as when running the model via SSH.

## with a Python Integrated Development Environment

This mode is most likely used when developing the generalized model logic and debugging the facility-specific logic.

# Installation

Installation partly depends on how the model will be run, as described above.

