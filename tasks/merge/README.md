# Merge task

This task requires merging many small expression matrices into one large one. We assume that each of the small matrices has been produced using an identical processing pipeline with the same reference, so the number and order of genes in each matrix is the same.

It addresses a particular problem for the HCA, where SmartSeq2 data is presently stored so that an expression matrix file only contains data for a single cell. But more generally, it involves performing relatively small reads from many different files.

The relevant data source is `GSE84465_split`, which contains expression data from 3000 cells split into 3000 different files. The expected output matrix should contain the expression values from all 3000 cells in any order.
