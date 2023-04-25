# Prefect Tools Package

A package of functions and tasks to expedite development of Prefect data flows.

## Installation

Install the latest version with pip from the repo: `oit_ds_prefect_tools @ git+https://github.com/UCBoulder/oit-ds-prefect-tools`

You can also add `@tag` to get a specific version

## Usage

To deploy a flow, after installing this package on your work station, run: `python3 flows/my_flow.py deploy`. This will deploy the flow using the `oit-ds-prefect-default:main` image.

Deployment images can be built using the https://github.com/UCBoulder/oit-ds-tools-prefect-images repo.

To deploy a flow using a dev version of the default image which you have created on `my-branch` of the prefect-images repo, do this: `python3 flows/my_flow.py deploy --image-branch my-branch`.

To deploy a flow using an image not named `oit-ds-prefect-default`, do this: `python3 flows/my_flow deploy --image-name oit-ds-prefect-my-image`

You can also combine both options if you want to test a dev version of a non-default image.
