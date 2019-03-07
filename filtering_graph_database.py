# First Level Filtering
# 	1- time budget specified by the user ---- this step needs to be translated in forms of a function in our data.
# 	Time budget could be constructed as a "LOSS" Function. Look at what Riddy did, maybe use his same function? What features work with this specific question?
# 	2- which are within the distance and are reachable within the time budget specified by the user.
# 	3- his motivated us to use Distance based filtering as the first step to reduce the the number of candidate POIs to be considered by the algorithm.
# 	If a user specifies the distance budget this filtering can be done in a more informed manner.
# 	Hence, first level filtering helps in considering only those POIs which can be visited in the tour duration in a realistic sense.
# 	Additionally, as the proposed algorithm is a graph based approach, reducing the number of candidate POIs results in a graph with reduced size.
# 	This in turn would drastically improve the runtime performance of the algorithm.


