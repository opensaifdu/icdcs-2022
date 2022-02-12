# icdcs-2022
simulation environment for icdcs-2022

To help interested readers understand our scheduling framework and the core algorithms, we extracted the core parts of our simulation environment and make them open source. Among them, the trace of vehicle motion and trace of computational tasks were generated as described in the paper.

# Project
vehicle.py: simulates the behavior of the vehicles and implements the computation offloading policy and the transmission scheduling policy.
edge_server.py: implements the distributed task dispatching policy, computing scheduling policy and result routing policy.
cloud.py: is responsible for managing the whole vehicular edge computing platform.
cluster_config.py: includes the configurations of each node in our proposed platform.
helloworld.proto: defines the communication formats in system.
