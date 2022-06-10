# Prefect Tools Package

A package of functions and tasks to expedite development of Prefect data flows.

## Installation

Install the latest version with pip from the repo: `oit_ds_prefect_tools @ git+https://github.com/UCBoulder/oit-ds-prefect-tools`

You can also add `@tag` to get a specific version

## Development

Clone to your local machine and create a new branch.

If making changes as part of another project, install oit_ds_prefect_tools in that project with `pip install -e path/to/repo`. This will allow you to edit this repo and have it automatically update in the installed project.

When ready, increment the version number is setup.cfg and merge into main. Tag the commit with the same version number and create a Github release for it.

Be sure to follow [Semantic Versioning](https://semver.org/).

## Deployment to Prefect Flows

To update the version of Prefect Tools used in the docker images of one or more Prefect flow repos (or the version of any Python package for that matter), the idea is to simply delete the cached images and rebuild them: assuming you import this package without a specific version number, rebuilding the image from scratch will result in pulling the latest version of the package.

### Updating packages in a single image

First, rebuild your local image with the `--no-cache` option and re-push it to Artifactory, for example:

```
cd image
docker build -f Dockerfile -t oit-data-services-docker-local.artifactory.colorado.edu/oit-ds-flows-ata:v1 --no-cache .
cd ..
make docker-image VERSION=v1
```

Then, re-build the image from the flow's repo on your local machine, for example: `make docker-image VERSION=v1`

Finally, run the same command from step 1 on the agent machine to remove the image and force Prefect to re-pull it next run from Artifactory.

### Updating packages in all images

If you make a bugfix in Prefect Tools and want to update all flows simultaneously to use the newest version, do this. If you want a particular flow to still use an old version of any package, specify the package version in your requirements.txt file.

First, remove all cached images from your local machine: `docker image prune -a`

Ensure you have all the flow repos for these images cloned to your computer, then rebuild the image for each one. For example (this assumes every image is on v1! grep carefully!):

`ls | grep oit-ds-flows | xargs -n 1 -I % sh 'cd % && make docker-image VERSION=v1'`

Finally, wait for a time when none of these docker images/flows are running on the agent machine, and then remove all cached images from it: `docker image prune -a`
