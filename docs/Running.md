# Setup a Proteus cluster

Follow the steps described to [build](Setup.md) the code across a cluster

# Run a Proteus cluster

Follow the steps described to [run](Single-Site-Test.md) the cluster.

These steps are for the deployment of a cluster all on a single node (two data sites, and an advisor). Deploying across a cluster is similar, just with changes to where the scripts execute, and changing hostname/mport pairs in config files.

By changing the benchmark configuration file and benchmark name to oltpbench, different benchmark workloads can be executed.

# Cracking

The experiments with cracking can be run by running the cracking [driver](../all-code/drivers/cracker.cpp), after building the source code.

Changing the gflags passed to the driver changes the experimental setup. For example, to run an experiment with only the SPAR predictor, run: `./cracker --scrack_do_prediction --scrack_eqp_type=SPAR`

## Notes

The scripts assume a certain configuration of repositories and directories. Moreover, these scripts were designed for a specific cluster configuration available at the University of Waterloo.

The terminology in the paper and the repository/source code are not identical, this is as a consequence of the source code deriving from previous projects. Common name mappings are:
- site manager: data site
- site selector: adaptive storage advisor
- horizondb/Adapt-HTAP: Proteus

