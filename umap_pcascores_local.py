import hail as hl
import pyspark

import umap
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from hail.plot import show
import bokeh
from bokeh.plotting import output_file, save
from bokeh.layouts import gridplot
import os

mtdir = "/Users/fs18/Desktop/projects/ddd_qc/sample_qc/matrixtables/"
pca_score_ht_file = mtdir + "mt_pca_scores.ht"
pca_score_ht = hl.read_table(pca_score_ht_file)

figdir = "/Users/fs18/Desktop/projects/ddd_qc/sample_qc/figures/"

#pca_score_ht.show(n=5)

pca_score_df = pca_score_ht.to_pandas()
pca_score_nplist = pca_score_df.scores.to_numpy()
pca_score_nparray = np.vstack(pca_score_nplist).astype(float)

pca_score_umap = umap.UMAP(n_components = 2, n_neighbors = 15, min_dist = 0.1, metric = 'euclidean').fit_transform(pca_score_nparray[:,:10])

pca_score_umap_df = pd.DataFrame(pca_score_umap)
pca_score_umap_df = pca_score_umap_df.rename(columns={0: "umap1", 1: "umap2"})
pca_score_umap_df['ega'] = pca_score_df.s
pca_score_umap_df.to_csv("/Users/fs18/Desktop/projects/ddd_qc/sample_qc/matrixtables/pca_scores_umap.txt", sep='\t', index=False)

pca_score_umap_ht = hl.import_table("/Users/fs18/Desktop/projects/ddd_qc/sample_qc/matrixtables/pca_scores_umap.txt", impute=True, key='ega')

plt.scatter(pca_score_umap[:,0], pca_score_umap[:,1], s=2, c='blue', marker='o', alpha=0.5, linewidths=0.1, edgecolors=None)
plt.savefig(figdir + "umap_pcascores.png")
plt.clf()

#################################################################

pop_ht_file = mtdir + "pop_assignments.ht"
pop_ht = hl.read_table(pop_ht_file)

kg_scores = pop_ht.filter(hl.is_defined(pop_ht.known_pop))
p1 = hl.plot.scatter(kg_scores.pca_scores[0], kg_scores.pca_scores[1], xlabel="PC1", ylabel="PC2", label=kg_scores.known_pop, title="Known populations - 1kg only, r2 = 0.1")
p2 = hl.plot.scatter(kg_scores.pca_scores[0], kg_scores.pca_scores[2], xlabel="PC1", ylabel="PC3", label=kg_scores.known_pop, title="Known populations - 1kg only, r2 = 0.1")

show(gridplot([p1, p2], ncols=2))

p3 = hl.plot.scatter(pop_ht.pca_scores[0], pop_ht.pca_scores[1], xlabel="PC1", ylabel="PC2", label=pop_ht.pop, title="Predicted populations, r2 = 0.1")
p4 = hl.plot.scatter(pop_ht.pca_scores[0], pop_ht.pca_scores[2], xlabel="PC1", ylabel="PC3", label=pop_ht.pop, title="Predicted populations, r2 = 0.1")

show(gridplot([p3, p4], ncols=2))

#################################################################

kg_scores_df = kg_scores.to_pandas()
kg_scores_nplist = kg_scores_df.pca_scores.to_numpy()
kg_scores_nparray = np.vstack(kg_scores_nplist).astype(float)

kg_scores_umap = umap.UMAP(n_components = 3, n_neighbors = 15, min_dist = 0.1, metric = 'euclidean').fit_transform(kg_scores_nparray[:,:10])
kg_scores_umap_df = pd.DataFrame(kg_scores_umap)
kg_scores_umap_df = kg_scores_umap_df.rename(columns={0:"umap1", 1:"umap2", 2:"umap3"})
kg_scores_umap_df['ega'] = kg_scores_df.s
kg_scores_umap_df.to_csv("/Users/fs18/Desktop/projects/ddd_qc/sample_qc/matrixtables/kg_scores_umap.txt", sep='\t', index=False)

kg_scores_umap_ht = hl.import_table("/Users/fs18/Desktop/projects/ddd_qc/sample_qc/matrixtables/kg_scores_umap.txt", impute=True, key='ega')

kg_scores = kg_scores.annotate(umap1 = kg_scores_umap_ht[kg_scores.s].umap1)
kg_scores = kg_scores.annotate(umap2 = kg_scores_umap_ht[kg_scores.s].umap2)
kg_scores = kg_scores.annotate(umap3 = kg_scores_umap_ht[kg_scores.s].umap3)

p5 = hl.plot.scatter(kg_scores.umap1, kg_scores.umap2, xlabel="UMAP1", ylabel="UMAP2", label=kg_scores.pop, title="Known populations - 1kg only, r2 = 0.1")
p6 = hl.plot.scatter(kg_scores.umap1, kg_scores.umap3, xlabel="UMAP1", ylabel="UMAP3", label=kg_scores.pop, title="Known populations - 1kg only, r2 = 0.1")

show(gridplot([p5, p6], ncols=2))

#################################################################

pop_ht_df = pop_ht.to_pandas()
pop_ht_nplist = pop_ht_df.pca_scores.to_numpy()
pop_ht_nparray = np.vstack(pop_ht_nplist).astype(float)

pop_ht_umap = umap.UMAP(n_components = 3, n_neighbors = 15, min_dist = 0.1, metric = 'euclidean').fit_transform(pop_ht_nparray[:,:10])
pop_ht_umap_df = pd.DataFrame(pop_ht_umap)
pop_ht_umap_df = pop_ht_umap_df.rename(columns={0:"umap1", 1:"umap2", 2:"umap3"})
pop_ht_umap_df['ega'] = pop_ht_df.s
pop_ht_umap_df.to_csv("/Users/fs18/Desktop/projects/ddd_qc/sample_qc/matrixtables/pop_ht_umap.txt", sep='\t', index=False)

pop_ht_umap_ht = hl.import_table("/Users/fs18/Desktop/projects/ddd_qc/sample_qc/matrixtables/pop_ht_umap.txt", impute=True, key='ega')

pop_ht = pop_ht.annotate(umap1 = pop_ht_umap_ht[pop_ht.s].umap1)
pop_ht = pop_ht.annotate(umap2 = pop_ht_umap_ht[pop_ht.s].umap2)
pop_ht = pop_ht.annotate(umap3 = pop_ht_umap_ht[pop_ht.s].umap3)

p7 = hl.plot.scatter(pop_ht.umap1, pop_ht.umap2, xlabel="UMAP1", ylabel="UMAP2", label=pop_ht.pop, title="Known populations - 1kg only, r2 = 0.1")
p8 = hl.plot.scatter(pop_ht.umap1, pop_ht.umap3, xlabel="UMAP1", ylabel="UMAP3", label=pop_ht.pop, title="Known populations - 1kg only, r2 = 0.1")

show(gridplot([p7, p8], ncols=2))

