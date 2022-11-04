# Prefect Tools Package

A package of functions and tasks to expedite development of Prefect data flows.

## Installation

Install the latest version with pip from the repo: `oit_ds_prefect_tools @ git+https://github.com/UCBoulder/oit-ds-prefect-tools`

You can also add `@tag` to get a specific version

## Deployment to Prefect Flows

To update the version of Prefect Tools used in the docker images of one or more Prefect flow repos (or the version of any Python package for that matter), all you have to do is rebuild the image without using the docker cache: assuming you import this package without a specific version number, rebuilding the image from scratch will result in pulling the latest version of the package.

To do this, simply use the [Image Builder Repo](https://github.com/UCBoulder/oit-ds-tools-image-builder)

If you rebuild a lot of images, you may want to run `docker system prune -a` on the agent machine to ensure storage is freed up.
