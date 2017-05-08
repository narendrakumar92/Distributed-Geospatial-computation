# Distributed-Geospatial-computation
We perform spatial operations on the dataset distributed over HDFS and Spark cluster consisting of a master and two worker nodes. We use New York City taxi dataset to obtain spatial statistics and use that to identify the statistically significant cell.

For Phase 1 description, please check CSE512-Phase1.pdf

Youtube Demo Phase 1: https://www.youtube.com/watch?v=oe5NLMuJWAI

For Phase 2 description, please check Phase2-requirement.pdf

A new function is created in GeoSpark Library to perform Naive Cartesian Product and the performance is compared with other optimized API calls. Full detailed analysis is reported (Phase2.pdf)


For Phase 3 description, please check Phase3-requirement.pdf.

We use New York City taxi dataset to find the important points in the envelope and create neighborhood of each cell in the space-time cube with latitude, longitude, and date (each cell has 26 neighbors), calculate significant cells using Getis-ord statistic (Hot Spot Analysis)

Full report: Final Report.pdf

