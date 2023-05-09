# Prefect Tools Package

A package of functions and tasks to expedite development of Prefect data flows.

## Installation

Install the latest version with pip from the repo: `ucb_prefect_tools @ git+https://github.com/UCBoulder/oit-ds-prefect-tools`

You can also add `@tag` to get a specific version

## Deploying Prefect Flows

In addition to providing Prefect tasks for common I/O operations, the Prefect Tools package also simplifies the deployment of Prefect flows with the `util.run_flow_command_line_interface` function. If your flow file is set up to automatically invoke the `util.run_flow_command_line_interface` function from the command line, you can deploy the flow like this: `python3 flows/my_flow.py deploy`.

To deploy a flow this way, you must have Git and Docker installed and you must be authenticated to Prefect Cloud via your Terminal. You will need to be at the root of a Git repository, and your active branch will determine how the flow is deployed. If you are on the main branch, the flow will be deployed with the `main` label which indicates a scheduled production flow. If you are on a non-main branch, the flow will be deployed with the `dev` label instead.

Within the Git repo, the `./flows` folder should contain flow definitions, with one Python file per flow. This folder can also contain additional files to be referenced by your flows, such as SQL files for longer queries. When any flow is deployed, this entire folder is copied into the flow storage for that deployment, minus anything excluded by the `.prefectignore` file.

Deployments created this way use KubernetesJob infrastructure and the flow storage defined by the `ds-flow-storage` JSON block in Prefect Cloud. The Docker registry, flow storage block name, and other settings can be overridden after importing the util package.

## Images for deployed flows

Deployment images can be built using the https://github.com/UCBoulder/oit-ds-tools-prefect-images repo.

The default image used for flows deployed with Prefect Tools is `oit-ds-prefect-default:main`.

To deploy a flow using a dev version of the default image which you have created on `my-branch` of the prefect-images repo, do this: `python3 flows/my_flow.py deploy --image-branch my-branch`.

To deploy a flow using an image not named `oit-ds-prefect-default`, do this: `python3 flows/my_flow deploy --image-name oit-ds-prefect-my-image`

You can also combine both options if you want to test a dev version of a non-default image.
