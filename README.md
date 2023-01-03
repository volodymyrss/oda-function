# ODA functions

## What do it do?

* Find functions from different sources
* Find suggestions for functions fitting purpose
* Call functions of different type
* in particular, call functions which generate functions

## Is this yet another workflow/function catalog

There are many workflow definitions and function catalogs. We do not need another one.
Which is why here there is no new standard for functions or catalogs: it is not possible to add functions to ODA itself.

Instead, this is a **homogeneous interface to existing function and catalog standards**.

## Workflow vs Function

## Features

* catalogs
    * locally constructed
    * TODO: oda notebooks from renkulab, github, gitlab
    * TODO: oda kg
    * TODO: published ld fragments
    * TODO: oda function collections for oda-experiments
    * TODO: ossr
    * TODO: workflowhub
    * TODO: dda catalogs
    * TODO: zenodo general
    * TODO: catalog sync and store

* function descriptions
    * python functions in the local code
    * python functions in files, from local path or http
    * TODO: api functions
    * TODO: oda notebooks
    * TODO: uri from rdf
    * TODO: containers
    * TODO: cwl
    * TODO: fno
    * TODO: module-function

* executors
    * local
    * TODO: reana
    * TODO: execution planner


* safety and performance
    * ensure hash, origin, version
    * use certified local copy if available

## Used by

* ddpapers
* oda notebook generation for renku
* oda-experiments for platform state
* oda-experiments for transient reactions
* cc-workflows
